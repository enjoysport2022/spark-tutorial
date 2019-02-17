package part3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataSources {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Data Sources")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

//    loading image data as DF
    val df = spark.read.format("image").option("dropInvalid", true)
      .load("/Users/didi/learn/learnSpark/src/main/resources/data/mllib/images/kittens")

    df.printSchema()

    df.select("image.origin", "image.width", "image.height").show(truncate = false)
    df.select("image.data").show()

  }

}
