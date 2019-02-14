import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//    http://www.cnblogs.com/MOBIN/p/5414490.html#12


object RDDsAccumulatorsBroadcastVars {

  object MyFunctions {
    def fun1(s: String): String = {
      "pre" + s
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RDDs, Accumulators, Broadcast Vars")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sc = spark.sparkContext

//    RDD
//    创建RDD: Parallelized
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

//    创建RDD: textFile
//    val distFile = sc.textFile("/Users/didi/learn/learnSpark/src/main/resources/ml-latest-small/README.txt")
    val distFile = sc.textFile("/Users/didi/learn/learnSpark/src/main/resources/people.txt")

//    操作
//    基础操作
    val lineLengths = distFile.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    lineLengths.persist()
    println(totalLength)

//    transformations（转换）
//    map
    val rdd = sc.parallelize(1 to 5)
    val map = rdd.map(x => x * 2)
    map.collect().foreach(x => print(x + " "))
    println()

//    flatMap
//    每个元素输入项都可以被映射到0个或多个的输出项，最终将结果”扁平化“后输出
    val fm = sc.parallelize(1 to 5)
          .flatMap(x => (1 to x))
    fm.take(10).foreach(x => print(x + " "))
    println()

//    采样: sample(withReplacement, fraction, seed)
    val sample1 = sc.parallelize(1 to 20)
      .sample(false, 0.5, 1)
    sample1.collect().foreach(x => print(x + " "))
    println()

//    RDD并集: union(ortherDataset)
    val unionRDD = sc.parallelize(1 to 3)
      .union(sc.parallelize(3 to 5))
    unionRDD.collect().foreach(x => print(x + " "))
    println()

//    RDD交集: intersection(otherDataset)
    val inRDD = sc.parallelize(1 to 3)
      .intersection(sc.parallelize(3 to 5))
    inRDD.collect().foreach(x => print(x + " "))
    println()

//    去重: distinct([numPartitions]))
    val disRDD = sc.parallelize(List(1, 2, 2, 3)).distinct()
    disRDD.collect().foreach(x => print(x + " "))
    println()

//    笛卡尔积操作: cartesian(otherDataset)
    val cartRDD = sc.parallelize(1 to 2)
      .cartesian(sc.parallelize(2 to 3))
    cartRDD.collect().foreach(x => print(x + " "))
    println()

//    filter
    val ft = sc.parallelize(1 to 5)
      .filter(x => x >2)
    ft.collect().foreach(x => print(x + " "))
    println()

//    Key-Value Pairs
    //    reduceByKey
    val counts = distFile.map(s => (s, 1))
      .reduceByKey((a, b) => a + b)
    println(counts.collect().foreach(println))

    //    sortByKey
    val scounts = distFile.map(s => (s, 1))
      .reduceByKey((a, b) => a + b)
      .sortByKey()
    println(scounts.collect().foreach(println))

    //    函数
    //    匿名函数
    val preFileLength = distFile.map(x => "pre" + x)
      .map(s => s.length)
      .reduce((a, b) => a + b)
    println(preFileLength)

    //    单例对象中的静态方法
    val funFileLength = distFile.map(MyFunctions.fun1)
      .map(s => s.length)
      .reduce((a, b) => a + b)
    println(funFileLength)


//    action（执行）

//    reduce(func):通过函数func先聚集各分区的数据集，再聚集分区之间的数据，
//    func接收两个参数，返回一个值，值再做为参数继续传递给函数func，直到最后一个元素
    val actionRDD = sc.parallelize(1 to 10, 2)
    val reduceRDD = actionRDD.reduce(_ + _)
    println(reduceRDD)

//    collect():以数据的形式返回数据集中的所有元素
    actionRDD.collect().foreach(x => print(x + " "))
    println()

//    count():返回数据集元素个数
    val countRDD = actionRDD.count()
    println(countRDD)

//    first():返回数据集的第一个元素
    val firstRDD = actionRDD.first()
    println(firstRDD)

//    take(n):以数组的形式返回数据集上的前n个元素
    val takeRDD = actionRDD.take(5)
    takeRDD.foreach(x => print(x + " "))
    println()

//    top(n):按默认或者指定的排序规则返回前n个元素，默认按降序输出
    val topRDD = actionRDD.top(3)
    topRDD.foreach(x => print(x + " "))
    println()

//    takeOrdered(n,[ordering]): 按自然顺序或者指定的排序规则返回前n个元素
    val takeOrderedRDD = actionRDD.takeOrdered(3)
    takeOrderedRDD.foreach(x => print(x + " "))
    println()


    val keyRDD = sc.parallelize(List(("a", 1), ("b", 2), ("a", 2), ("b", 3)))

//    countByKey():作用于K-V类型的RDD上，统计每个key的个数，返回(K,K的个数)
    val countByKeyRDD = keyRDD.countByKey()
    countByKeyRDD.foreach(x => print(x + " "))
    println()

//    collectAsMap():作用于K-V类型的RDD上，作用与collect不同的是collectAsMap函数不包含重复的key，对于重复的key。后面的元素覆盖前面的元素
    val collectAsMapRDD = keyRDD.collectAsMap()
    collectAsMapRDD.foreach(x => print(x + " "))
    println()

//    lookup(k)：作用于K-V类型的RDD上，返回指定K的所有V值
    val lookupRDD = keyRDD.lookup("a")
    lookupRDD.foreach(x => print(x + " "))
    println()

//    saveAsTextFile(path): 将结果保存到指定的HDFS目录中


//    共享变量
//    Broadcast variables（广播变量）
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    broadcastVar.value.foreach(x => print(x + " "))
    println()

//    Accumulators（累加器）
    val accum = sc.longAccumulator("my Accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    println(accum.value)

    /*
     */


  }

}
