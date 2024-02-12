from pyspark.sql import SparkSession

if __name__ == "__main__":
    # SparkSessionを作成
    spark = SparkSession.builder \
        .appName("Spark BigQuery Example") \
        .config("spark.jars", "./spark-3.5-bigquery-0.36.1.jar") \
        .getOrCreate()

    # BigQueryテーブルに対するクエリを実行
    df = spark.read.format("bigquery") \
        .option("table", "replog-359915.test_dataset.test_table") \
        .option("credentialsFile", "") \
        .load()

    # データフレームに対する操作を実行
    df.show()

    # DataFrameをCSV形式に変換してCloud Storageにアップロード
    output_path = "gs://spark_output_bucket/output.csv"
    # df.write.mode("overwrite").csv(output_path)
    df.write.format("csv").mode("overwrite").save(output_path)
    # SparkSessionを停止
    spark.stop()