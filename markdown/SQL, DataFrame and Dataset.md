# Spark官方文档学习-SQL, DataFrame and Dataset

- [原文链接](http://spark.apache.org/docs/latest/sql-getting-started.html)

- [git地址](https://github.com/poteman/learnSpark)

- xmind思维导图

- ![xmind思维导图](https://ws1.sinaimg.cn/large/006tNc79ly1g04o1osv2sj310w0u00xf.jpg)

- SparkSession

  - 创建SparkSession

  - ``````scala
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    ``````

  - 读取文件

    - json文件

      ```
      val df = spark.read.json("examples/src/main/resources/people.json")
      // 返回为Dataframe
      ```

    - txt文件

      ```
      val peopleDF = spark.sparkContext
        .textFile("examples/src/main/resources/people.txt")
      // 返回为rdd
      ```

- 结果消除info日志

  ```
  import org.apache.log4j.{Level, Logger}
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  ```

- DataFrame

  - 创建DF

  - ```
    val df = spark.read.json("examples/src/main/resources/people.json")
    ```

  - DF操作

  - ```
    // 操作DF需要导入
    import spark.implicits._
    ```

    - 显示

      - 显示内容

      - ```
        df.show()
        ```

      - 查看结构

      - ```
        df.printSchema()
        ```

    - 列选择

      - 选择其中一列

      - ```
        df.select("name").show()
        ```

      - 选择其中几列

      - ```
        df.select("name", "age").show()
        ```

    - 列操作

      - 利用$

      - ```
        df.select($"name", $"age" + 1).show()
        ```

      - 利用map

        - 通过index

        - ```
          df.map(teenager => {"Name: " + teenager(1)}).show()
          ```

        - 通过列名

        - ```
          df.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
          ```

      - 利用withColumn

      - ```
        import org.apache.spark.sql.functions.col
        df.withColumn("age+1",col("age")+1).show()
        ```

    - 行选择

      - filter

      - ```
        df.filter($"age" > 21).show()
        ```

      - where

      - ```
        df.where("age is not null")
        ```

    - 聚合函数

      - 聚合获得一个结果

      - ```
        df.groupBy("age").count().show()
        ```

      - 聚合获得多个结果

      - ```
        df.groupBy(col("age"))
          .agg(
          max(col("name")).alias("name_max"),
          min(col("name")).alias("name_min") )
        ```

    - 空值填充

    - ```
      df.na.fill(0.0)
      ```

    - 视图View

      - 临时视图（session级别）

        - 创建

        - ```
          df.createOrReplaceTempView("people")
          ```

        - 查询

        - ```
          val sqlDF = spark.sql("SELECT * FROM people")
          ```

      - 全局视图（应用级别）, 存在于系统数据库global_temp中

        - 创建

        - ```
          df.createGlobalTempView("people")
          ```

        - 查询

          - session内查询

          - ```
            spark.sql("SELECT * FROM global_temp.people").show()
            ```

          - 跨session查询

          - ```
            spark.newSession().sql("SELECT * FROM global_temp.people").show()
            ```

  - rdd转DF

    - toDF函数

    - ```
      case class Person(name: String, age: Long)
      val peopleDF = spark.sparkContext
        .textFile("examples/src/main/resources/people.txt")
        .map(_.split(","))
        .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
        .toDF()
      ```

    - Schema

    - ```
      import org.apache.spark.sql.types._
      import org.apache.spark.sql.Row
      val rowRDD = spark.sparkContext.textFile("/Users/didi/打车金/asd/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))
      val schema = StructType(Array(StructField("name", StringType, true), StructField("age", IntegerType, true)))
      val schemaDF = spark.createDataFrame(rowRDD, schema)
      ```

- Dataset

  - 创建DS

    - Seq

    - ```
      val caseClassDS = Seq(Person("Andy", 32)).toDS()
      val primitiveDS = Seq(1, 2, 3).toDS()
      ```

    - Json

    - ```
      case class Person(name: String, age: Long)
      val path = "examples/src/main/resources/people.json"
      val peopleDS = spark.read.json(path).as[Person]
      ```

  - DS操作

  - ```
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    ```

- UDF

  - Sql

  - ```
    // AggregateFunctions.scala
    
    import org.apache.log4j.{Level, Logger}
    import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{Row, SparkSession}
    
    object AggregateFunctions {
    
      object MyAverage extends UserDefinedAggregateFunction {
        // Data types of input arguments of this aggregate function
        def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
        // Data types of values in the aggregation buffer
        def bufferSchema: StructType = {
          StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
        }
        // The data type of the returned value
        def dataType: DataType = DoubleType
        // Whether this function always returns the same output on the identical input
        def deterministic: Boolean = true
        // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
        // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
        // the opportunity to update its values. Note that arrays and maps inside the buffer are still
        // immutable.
        def initialize(buffer: MutableAggregationBuffer): Unit = {
          buffer(0) = 0L
          buffer(1) = 0L
        }
        // Updates the given aggregation buffer `buffer` with new input data from `input`
        def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
          if (!input.isNullAt(0)) {
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1
          }
        }
        // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
        def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
          buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
          buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        }
        // Calculates the final result
        def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
      }
    
      def main(args: Array[String]): Unit = {
    
        val spark = SparkSession
          .builder()
          .master("local")
          .appName("Spark SQL basic example")
          .config("spark.some.config.option", "some-value")
          .getOrCreate()
    
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)
    
        spark.udf.register("myAverage", MyAverage)
    
        //    Creating DataFrames
        val df = spark.read.json("/Users/didi/learn/learnSpark/src/main/resources/employees.json")
        df.createOrReplaceTempView("employees")
        df.show()
    
        val result = spark.sql("select myAverage(salary) as average_salary from employees")
        result.show()
    
      }
    
    }
    ```

  - DataSet

  - ```
    // AggregateFunctions2.scala
    import org.apache.log4j.{Level, Logger}
    import org.apache.spark.sql.expressions.Aggregator
    import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
    
    object AggregateFunctions2 {
    
      case class Employee(name: String, salary: Long)
      case class Average(var sum: Long, var count: Long)
    
      object MyAverage extends Aggregator[Employee, Average, Double] {
    
        def zero: Average = Average(0L, 0L)
    
        def reduce(buffer: Average, employee: Employee): Average = {
          buffer.sum += employee.salary
          buffer.count += 1
          buffer
        }
    
        def merge(b1: Average, b2: Average): Average ={
          b1.sum += b2.sum
          b1.count += b2.count
          b1
        }
    
        def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
    
        def bufferEncoder: Encoder[Average] = Encoders.product
    
        def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    
      }
    
      def main(args: Array[String]): Unit = {
    
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
          val ds = spark.read.json("/Users/didi/learn/learnSpark/src/main/resources/employees.json").as[Employee]
          ds.show()
    
          val averageSalary = MyAverage.toColumn.name("average_salary")
          val result = ds.select(averageSalary)
    
          result.show()
    
    
      }
    
    }
    
    ```

- 