import argparse

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="input filename", default="testset.csv")
    parser.add_argument(
        "-o", "--output", help="output filename", default="testset.delta"
    )
    parser.add_argument("--s3credential", help="s3 credential provider")
    parser.add_argument("--s3endpoint", help="s3 endpoint")
    parser.add_argument("--s3accesskey", help="s3 accesskey")
    parser.add_argument("--s3secretkey", help="s3 secretkey")
    parser.add_argument(
        "-p",
        "--partitions",
        help="delta table partitions (e.g. date, ...)",
        nargs="*",
        default=["date"],
    )
    parser.add_argument("-r", "--rowcounts", help="row counts per append", type=int)
    args = parser.parse_args()

    so = SparkConf()
    so.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    so.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    if args.s3credential == "simple":
        so.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        so.set("spark.hadoop.fs.s3a.endpoint", args.s3endpoint)
        so.set("spark.hadoop.fs.s3a.access.key", args.s3accesskey)
        so.set("spark.hadoop.fs.s3a.secret.key", args.s3secretkey)
    elif args.s3credential == "profile":
        so.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider",
        )
    so.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    so.set("spark.hadoop.fs.s3a.path.style.access", "true")
    so.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")

    ss = SparkSession.builder.appName("csvtodelta").config(conf=so).getOrCreate()

    df = ss.read.format("csv").option("header", True).load(args.input)

    if args.rowcounts:
        for i in range(0, df.count(), args.rowcounts):
            tf = df.limit(args.rowcounts)
            df = df.subtract(tf)
            tf.write.format("delta").mode("append").partitionBy(*args.partitions).save(
                args.output
            )
    else:
        df.write.format("delta").mode("append").partitionBy(*args.partitions).save(
            args.output
        )
