import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object Facebook {
  def main(args: Array[String]) = {
    val sc = getSC()
    val FB_count = FBCount(sc)
    saveit(FB_count)
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    sc
 }
  def FBCount(sc: SparkContext) = {
    val input = sc.textFile("/datasets/facebook")
    val filteredRDD = input.map(_.split(" "))
                           .map(x => (x(0), x(1).toFloat))
                           .filter{case (user1, user2) => user2 > 500}
                           .map{case (user1, user2) => (user1, 1)}
    val FBCount = filteredRDD.reduceByKey((x,y) => x+y)
                                 .filter{case (user1, count) => count > 2}
                                 .map{case (user1, count) => (user1, count +1)}
    FBCount
  }
  def saveit(FB_count: org.apache.spark.rdd.RDD[(String, Int)]) = {
    FB_count.saveAsTextFile("FBCountRDD")
  }

}
