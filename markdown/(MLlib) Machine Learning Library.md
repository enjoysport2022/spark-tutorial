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

- Data sources

  - loading image data as DF(spark 2.4之后的版本才有)

  - ```
    val df = spark.read.format("image").option("dropInvalid", true)
      .load("/Users/didi/learn/learnSpark/src/main/resources/data/mllib/images/kittens")
    ```

- Pipelines

  - Estimator, Transformer, Param

  - ```
    // 创建一个LR实例，该实例就是一个Estimator.
    val lr = new LogisticRegression()
    
    //设置参数
    lr.setMaxIter(10)
      .setRegParam(0.01)
      
    val model1 = lr.fit(training)
    
    // 使用ParamMap来设置参数
    val paramMap = ParamMap(lr.maxIter -> 20)
          .put(lr.maxIter, 30)
          .put(lr.regParam -> 0.1, lr.threshold -> 0.55)
          
    // 合并两个ParamMap
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ paramMap2
    
    val model2 = lr.fit(training, paramMapCombined)
    
    val pre = model2.transform(test)
    ```

  - Pipeline

  - ```
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")
    
    val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
    
    val hashingTF = new HashingTF()
        .setNumFeatures(1000)
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
    
    val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.001)
    
    val pipeline = new Pipeline()
          .setStages(Array(tokenizer, hashingTF, lr))
    
    val model = pipeline.fit(training)
    
    // 模型保存
    model.write.overwrite().save("/Users/didi/learn/learnSpark/src/main/scala/part3/modelSave/lr-model")
    // pipeline保存
    pipeline.write.overwrite().save("/Users/didi/learn/learnSpark/src/main/scala/part3/modelSave/pipeline1")
    
    val test = spark.createDataFrame(Seq(
        (4L, "spark i j k"),
        (5L, "l m n"),
        (6L, "spark hadoop spark"),
        (7L, "apache hadoop")
        )).toDF("id", "text")
    
    val pre = model.transform(test)
    ```

- 
