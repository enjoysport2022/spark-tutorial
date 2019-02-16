package part2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SqlDataFrameAndDataset {

  def main(args: Array[String]): Unit = {
    //    starting point: SparkSession
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    //    Creating DataFrames
    val df = spark.read.json("/Users/didi/learn/learnSpark/src/main/resources/people.json")
    df.show()

    //    Untyped Dataset Operations
    df.printSchema()
    df.select("name", "age").show()
    df.select($"name", $"age" + 1).show()
    df.map(teenager => {"Name: " + teenager(1)}).show()
    df.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    df.withColumn("age+1",col("age")+1).show()


    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    //    Running SQL Queries
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

    //    Global Temporary View
    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
    spark.newSession().sql("select * from global_temp.people").show()

    //    Creating Datasets
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    val primitiveDS = Seq(1,2,3).toDS()
    primitiveDS.map(_ + 1).collect()

    val path = "/Users/didi/learn/learnSpark/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

    val peopleDF = spark.sparkContext
      .textFile("/Users/didi/learn/learnSpark/src/main/resources/people.txt")
      .map(_.split(", "))
      .map(attributes => Person(attributes(0), attributes(1).toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("select name, age from people where age between 13 and 19")
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

//    通过schema创建DF
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    val rowRDD = spark.sparkContext.textFile("/Users/didi/learn/learnSpark/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))
    val schema = StructType(Array(StructField("name", StringType, true), StructField("age", IntegerType, true)))
    val schemaDF = spark.createDataFrame(rowRDD, schema)
    schemaDF.printSchema()

  }
}
