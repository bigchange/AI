package com.bigchange.ssql

/**
  * Created by C.J.YOU on 2016/11/18.
  * Spark sql Practice - SparkSession
  */
object DataSetPractice {

 /* val warehouseLocation = "spark-warehouse"

  val spark = Spark.apply("local", "SSQL", 60).sessionBuilder
    .config("spark.some.config.option", "some-value")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()

  // This is used to implicitly convert an RDD to a DataFrame.
  import spark.implicits._

  // Run SQL on files directly
  val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")

  // Create a simple DataFrame, store into a partition directory
  val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
  squaresDF.write.parquet("data/test_table/key=1")

  // Create another DataFrame in a new partition directory,
  // adding a new column and dropping an existing column
  val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
  cubesDF.write.parquet("data/test_table/key=2")

  // Read the partitioned table
  val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
  mergedDF.printSchema()

  // spark is an existing SparkSession
  // Metadata Refreshing
  spark.catalog.refreshTable("my_table")

  import spark.sql

  sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
  sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

  // jdbc
  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql:dbserver")
    .option("dbtable", "schema.tablename")
    .option("user", "username")
    .option("password", "password")
    .load()*/

}
