import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.ClassTag
object ZZY1120200325 {
  def main(args: Array[String]): Unit = {
    // TODO 建立和Spark框架的连接
    val conf = new SparkConf().setAppName("KMerCount").setMaster("local")
    //创建一个SparkConf对象,设置应用程序的名称为"KMerCount",运行模式为本地模式
    val sc = new SparkContext(conf)
    //创建一个SparkContext对象,使用上述的SparkConf配置

    // TODO 执行输入操作
    val input = sc.textFile("KM-input.txt")
    //读取文件，获取一行一行的数据
    val k = 2
    val n = 10
    //按题目要求设置参数

    // TODO 执行计数操作
    val kmerCounts = input.flatMap(str => {
      //用kmerCounts存储下列步骤的运算结果
      val kmers = for (i <- 0 until str.length - k + 1) yield str.substring(i, i + k)
      //使用for循环生成一个包含所有长度为k的子字符串的序列kmers,比如ABCDA,K=2,那么子串包含AB,BC,CD,DA
      kmers.map(kmer => (kmer, 1))
      //对序列kmers中的每个元素kmer,使用map作将其转换为元组(kmer,1),其中1表示每个kmer出现的初始计数为1
    }).reduceByKey(_ + _)
      //对生成的元组列表执行reduceByKey操作,根据元组的第一个元素kmer进行分组,并将相同kmer的计数值进行累加,得到每个K-mer出现的次数

    // TODO 执行排序操作
    val topN = kmerCounts.map(_.swap)
      //对kmerCounts进行映射操作，将每个元素的键值对中的key和value进行交换,得到新的(count,kmer)集合
      .sortBy(identity)(Ordering.Tuple2(Ordering.Int.reverse, Ordering.String.reverse), ClassTag[(Int, String)](classOf[(Int, String)]))
      //使用.sortBy方法排序：Ordering.Int.reverse表示按照整数降序排序,Ordering.String.reverse表示按照字符串降序排序
      //首先按照第一个元素（整数）进行降序排序，若两元素相同,按照第二个元素（字符串）进行降序排序
      //语法介绍：
      //Tuple2 是 Scala 标准库中表示包含两个元素的元组（tuple）的类型。它是一个泛型类，用于表示具有固定顺序的两个值的组合
      //.sortBy方法的规范的写法为：collection.sortBy(keyFunction)(implicit ordering,implicit classTag)
      //collection：要排序的集合，可以是 List、Array、Seq 等类型的集合
      //keyFunction：排序关键字函数，用于从集合的每个元素中提取排序依据
      //ordering：隐式的排序规则，用于指定元素的排序方式。可以使用 Ordering 对象提供的方法或自定义的排序规则
      //classTag：隐式的类型标签，用于指定排序结果的类型。通常使用 ClassTag 对象来指定泛型类型
      .take(n)
      //取排序后的前n个元素
      .map(_.swap)
      //再次交换key和value,得到(kmer,count)集合

    // TODO 计算平均值
    val avgCount = kmerCounts.map(_._2).mean()
    //计算kmerCounts中计数值的平均值

    // TODO 输出(1)
    topN.foreach(println)

    // TODO 输出(2)
    println(avgCount)

    // TODO 关闭连接
    sc.stop()

  }
}
