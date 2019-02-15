# Spark从入门到精通 (MLlib) Machine Learning Library

- [官方原文链接](http://spark.apache.org/docs/latest/ml-guide.html)

- 本文代码对应的[git地址](https://github.com/poteman/spark-tutorial)

- ![本文知识点](https://ws4.sinaimg.cn/large/006tKfTcly1g066kx6jb9j30u00uv0y1.jpg)

- [思维导图源文件](https://github.com/poteman/learnSpark/blob/master/xmind/RDDs%2C%20Accumulators%2C%20Broadcast%20Vars.xmind)

- 初始化spark并消除结果info日志

  ```
  val spark = SparkSession
  .builder()
  .master("local")
  .appName("Basic Statistics")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
  import spark.implicits._
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  ```

- Basic Statistics

  - Correlation

  - ```
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    
    val df = data.map(Tuple1.apply).toDF("features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")
    
    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")
    ```

  - Hypothesis testing

  - ```
    val data2 = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )
    val df2 = data2.toDF("label", "features")
    val chi = ChiSquareTest.test(df2, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
    ```

  - Summarizer

  - ```
    import Summarizer._
    val data3 = Seq(
      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
    )
    val df3 = data3.toDF("features", "weight")
    val (meanVal, varianceVal) = df3.select(metrics("mean", "variance")
      .summary($"features", $"weight").as("summary"))
      .select("summary.mean", "summary.variance")
      .as[(Vector, Vector)].first()
    
    println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")
    
    val (meanVal2, varianceVal2) = df3.select(mean($"features"), variance($"features"))
      .as[(Vector, Vector)].first()
    
    println(s"without weight: mean = ${meanVal2}, sum = ${varianceVal2}")
    ```

- 

- 
