# Spark从入门到精通 RDDs, Accumulators, BroadcastVars

- [官方原文链接](http://spark.apache.org/docs/latest/rdd-programming-guide.html#working-with-key-value-pairs)

- 本文代码对应的[git地址](https://github.com/poteman/learnSpark/blob/master/src/main/scala/RDDsAccumulatorsBroadcastVars.scala)

- ![本文知识点](https://ws4.sinaimg.cn/large/006tKfTcly1g066kx6jb9j30u00uv0y1.jpg)

- [思维导图源文件](https://github.com/poteman/learnSpark/blob/master/xmind/RDDs%2C%20Accumulators%2C%20Broadcast%20Vars.xmind)

- 初始化spark并消除结果info日志

  ```
  val spark = SparkSession
        .builder()
        .master("local")
        .appName("RDDs, Accumulators, Broadcast Vars")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  val sc = spark.sparkContext
  ```

- 创建RDD
  - parallelize
  ```
  val data = Array(1, 2, 3, 4, 5)
  val distData = sc.parallelize(data)

  val rdd = sc.parallelize(1 to 5)
  ```
  - 外部文件(textFile)
  ```
  val distFile = sc.textFile("/Users/didi/learn/learnSpark/src/main/resources/people.txt")
  ```

- RDD操作
  - 打印
    ```
    rdd.foreach(x => print(x + " "))
    rdd.collect().foreach(println)
    rdd.take(n).foreach(println)
    ```

  - transformations（转换）：延迟计算

    - map

      - map

        ```
        val rdd = sc.parallelize(1 to 5)
        val map = rdd.map(x => x * 2)
        ```

      - flatMap: 每个元素输入项都可以被映射到0个或多个的输出项，最终将结果”扁平化“后输出

        ```
        val fm = sc.parallelize(1 to 5)
              .flatMap(x => (1 to x))
        ```

    - 采样: sample(withReplacement, fraction, seed)

      ```
      val sample1 = sc.parallelize(1 to 20)
        .sample(false, 0.5, 1)
      ```

    - RDD交并集
      - 并集: union(ortherDataset)

        ```
        val unionRDD = sc.parallelize(1 to 3)
          .union(sc.parallelize(3 to 5))
        ```

      - 交集: intersection(otherDataset)

        ```
        val inRDD = sc.parallelize(1 to 3)
          .intersection(sc.parallelize(3 to 5))
        ```

    - 去重: distinct

      ```
      val disRDD = sc.parallelize(List(1, 2, 2, 3)).distinct()
      ```

    - 笛卡尔积操作: cartesian(otherDataset)

      ```
      val cartRDD = sc.parallelize(1 to 2)
        .cartesian(sc.parallelize(2 to 3))
      ```

    - 筛选: filter

      ```
      val ft = sc.parallelize(1 to 5)
        .filter(x => x >2)
      ```

    - Key-Value

      - reduceByKey

        ```
        val counts = distFile.map(s => (s, 1))
          .reduceByKey((a, b) => a + b)
        ```

      - sortByKey

        ```
        val scounts = distFile.map(s => (s, 1))
          .reduceByKey((a, b) => a + b)
          .sortByKey()
        ```

  - actions（执行）触发计算

    - reduce: 通过函数func先聚集各分区的数据集，再聚集分区之间的数据，func接收两个参数，返回一个值，值再做为参数继续传递给函数func，直到最后一个元素

      ```
      val actionRDD = sc.parallelize(1 to 10, 2)
      val reduceRDD = actionRDD.reduce(_ + _)
      ```

    - collect: 以数据的形式返回数据集中的所有元素

      ```
      actionRDD.collect().foreach(x => print(x + " "))
      ```

    - count: 返回数据集元素个数

      ```
      val countRDD = actionRDD.count()
      ```

    - first: 返回数据集的第一个元素

      ```
      val firstRDD = actionRDD.first()
      ```

    - take(n): 以数组的形式返回数据集上的前n个元素

      ```
      val takeRDD = actionRDD.take(5)
      ```

    - top(n): 按默认或者指定的排序规则返回前n个元素，默认按降序输出

      ```
      val topRDD = actionRDD.top(3)
      ```

    - takeOrdered(n,[ordering]): 按自然顺序或者指定的排序规则返回前n个元素

      ```
      val takeOrderedRDD = actionRDD.takeOrdered(3)
      ```

    - Key-Value

      ```
      val keyRDD = sc.parallelize(List(("a", 1), ("b", 2), ("a", 2), ("b", 3)))
      ```

      - countByKey: 统计每个key的个数

        ```
        val countByKeyRDD = keyRDD.countByKey()
        ```

      - collectAsMap: 不包含重复的key

        ```
        val collectAsMapRDD = keyRDD.collectAsMap()
        ```

      - lookup(k): 查找指定K的所有V值

        ```
        val lookupRDD = keyRDD.lookup("a")
        ```

    - saveAsTextFile(path): 将结果保存到指定的HDFS目录中

      ```
      rdd.saveAsTextFile(path)
      ```

  - persist（持久化）

    - persist()

      ```
      rdd.persist()
      ```

    - cache()

      ```
      rdd.cache()
      ```

  - Functions（函数）

    - 匿名函数

      ```
      val preFileLength = distFile.map(x => "pre" + x)
        .map(s => s.length)
        .reduce((a, b) => a + b)
      ```

    - 单例对象中的静态方法

      ```
      object MyFunctions {
          def fun1(s: String): String = {
            "pre" + s
          }}
      val funFileLength = distFile.map(MyFunctions.fun1)
        .map(s => s.length)
        .reduce((a, b) => a + b)
      ```

  - Shuffle 操作: shuffle 是spark 重新分配数据的一种机制，使得这些数据可以跨不同的区域进行分组。这通常涉及在 executors 和 机器之间拷贝数据，这使得 shuffle 成为一个复杂的、代价高的操作。包括repartition, coalesce, groupByKey, reduceByKey, cogroup, join等

  - 删除数据

    ```
    RDD.unpersist()
    ```

- 共享变量 shared variables

  - Broadcast Vars(广播变量)

    ```
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    ```

  - Accumulators(累加器)

    ```
    val accum = sc.longAccumulator("my Accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    ```
