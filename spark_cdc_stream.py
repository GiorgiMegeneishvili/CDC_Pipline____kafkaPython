"""
Kafka -> Spark Structured Streaming -> console
ერთი ფაილი ყველა ცხრილისთვის: ცხრილები TABLES config-შია, ლოგიკა ერთხელ წერია.
თითო ცხრილი = ცალკე stream + ცალკე checkpoint (ერთის შეცდომა სხვას არ აჩერებს).

გაშვება:
    python spark_cdc_stream.py
"""

import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, DateType, TimestampType, IntegerType, LongType, StringType,
)

# =====================================================================
# CONFIG
# =====================================================================
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_PREFIX = "postgres.bronze"
CHECKPOINT_ROOT = "./checkpoints"

# რომელი ცხრილები გაეშვას (console-ზე 8 ერთად ხმაურიანია — დასაწყისში 1-2 აირჩიე)
TABLES_TO_RUN = ["nw1_shippers", "nw1_orders","nw1_customers","nw1_employees",
                 "nw1_products","nw1_categories","nw1_suppliers","nw1_order_details"]

# pk   = ბიზნეს გასაღები (MERGE-ისთვის დაგვჭირდება)
# cols = სვეტები Spark DDL-ით. ტიპები შენი Postgres DDL-ს შეუსაბამე!
#        DATE / TIMESTAMP ავტომატურად გარდაიქმნება Debezium-ის რიცხვებიდან.
#        NUMERIC -> DOUBLE (decimal.handling.mode=double), bytea -> STRING (base64)
TABLES = {
    "nw1_customers": {
        "pk": ["customerid"],
        "cols": """customerid STRING, companyname STRING, contactname STRING, contacttitle STRING,
                   address STRING, city STRING, region STRING, postalcode STRING, country STRING,
                   phone STRING, fax STRING, load_date DATE""",
    },
    "nw1_employees": {
        "pk": ["employeeid"],
        "cols": """employeeid INT, lastname STRING, firstname STRING, title STRING, titleofcourtesy STRING,
                   birthdate TIMESTAMP, hiredate TIMESTAMP, address STRING, city STRING, region STRING,
                   postalcode STRING, country STRING, homephone STRING, extension STRING, photo STRING,
                   notes STRING, reportsto INT, photopath STRING, load_date DATE""",
    },
    "nw1_products": {
        "pk": ["productid"],
        "cols": """productid INT, productname STRING, supplierid INT, categoryid INT, quantityperunit STRING,
                   unitprice DOUBLE, unitsinstock INT, unitsonorder INT, reorderlevel INT,
                   discontinued BOOLEAN, load_date DATE""",
    },
    "nw1_categories": {
        "pk": ["categoryid"],
        "cols": """categoryid INT, categoryname STRING, description STRING, picture STRING, load_date DATE""",
    },
    "nw1_suppliers": {
        "pk": ["supplierid"],
        "cols": """supplierid INT, companyname STRING, contactname STRING, contacttitle STRING, address STRING,
                   city STRING, region STRING, postalcode STRING, country STRING, phone STRING, fax STRING,
                   homepage STRING, load_date DATE""",
    },
    "nw1_shippers": {
        "pk": ["shipperid"],
        "cols": """shipperid INT, companyname STRING, phone STRING, load_date DATE""",
    },
    "nw1_orders": {
        "pk": ["orderid"],
        "cols": """orderid INT, customerid STRING, employeeid INT, orderdate TIMESTAMP, requireddate TIMESTAMP,
                   shippeddate TIMESTAMP, shipvia INT, freight DOUBLE, shipname STRING, shipaddress STRING,
                   shipcity STRING, shipregion STRING, shippostalcode STRING, shipcountry STRING,
                   load_date DATE""",
    },
    "nw1_order_details": {
        "pk": ["orderid", "productid"],
        "cols": """orderid INT, productid INT, unitprice DOUBLE, quantity INT, discount DOUBLE,
                   load_date DATE""",
    },
}

# =====================================================================
# SPARK
# =====================================================================

SPARK_VER = pyspark.__version__
SCALA_VER = "2.13" if SPARK_VER.startswith("4") else "2.12"

spark = (
    SparkSession.builder
    .appName("cdc_nw1_stream")
    .master("local[*]")
    .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VER}:{SPARK_VER}")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")   # Debezium timestamp-ები UTC-ად მოდის
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# =====================================================================
# GENERIC ლოგიკა
# =====================================================================
def target_schema(ddl: str) -> StructType:
    """DDL სტრიქონი -> StructType (საბოლოო ტიპები)."""
    return spark.createDataFrame([], ddl).schema


def debezium_schema(target: StructType) -> StructType:
    """Debezium JSON-ში DATE = დღეები (int), TIMESTAMP = მიკროწამები (long)."""
    fields = []
    for f in target.fields:
        if isinstance(f.dataType, DateType):
            fields.append(StructField(f.name, IntegerType()))
        elif isinstance(f.dataType, TimestampType):
            fields.append(StructField(f.name, LongType()))
        else:
            fields.append(f)
    return StructType(fields)


def convert_col(field: StructField, prefix: str):
    c = f"{prefix}.{field.name}"
    if isinstance(field.dataType, DateType):
        return F.expr(f"date_add(DATE'1970-01-01', {c})")
    if isinstance(field.dataType, TimestampType):
        return F.expr(f"timestamp_micros({c})")
    return F.col(c)


def build_stream(table: str):
    cfg = TABLES[table]
    target = target_schema(cfg["cols"])
    row_schema = debezium_schema(target)

    envelope = StructType([
        StructField("before", row_schema),
        StructField("after", row_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("source", StructType([
            StructField("lsn", LongType()),
            StructField("txId", LongType()),
        ])),
    ])

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        # subscribePattern: topic-ი ჯერ თუ არ არსებობს, არ ჩავარდება —
        # დაელოდება და პირველი event-ის შემდეგ ავტომატურად აიღებს
        .option("subscribePattern", f"^{TOPIC_PREFIX}.{table}$".replace(".", r"\."))
        .option("kafka.metadata.max.age.ms", "10000")   # ახალ topic-ს 10 წამში აღმოაჩენს (default 5 წთ)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")     # retention-ით წაშლილ offset-ებზე არ ჩავარდეს
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), envelope).alias("e"))
        .where(F.col("e").isNotNull())
    )

    # PK: delete-ზე after = null, ამიტომ before-დან ვიღებთ
    pk_cols = [
        F.coalesce(F.col(f"e.after.{k}"), F.col(f"e.before.{k}")).alias(k)
        for k in cfg["pk"]
    ]
    data_cols = [
        convert_col(f, "e.after").alias(f.name)
        for f in target.fields if f.name not in cfg["pk"]
    ]

    events = parsed.select(
        F.col("e.op").alias("op"),
        F.col("e.source.lsn").alias("lsn"),
        F.timestamp_millis(F.col("e.ts_ms")).alias("event_ts"),
        *pk_cols,
        *data_cols,
    )

    return (
        events.writeStream
        .queryName(table)
        .format("console")
        .option("truncate", False)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table}")
        .trigger(processingTime="5 seconds")
        .start()
    )


# =====================================================================
# RUN
# =====================================================================
if __name__ == "__main__":
    for t in TABLES_TO_RUN:
        build_stream(t)
        print(f"started stream: {t}")

    spark.streams.awaitAnyTermination()
