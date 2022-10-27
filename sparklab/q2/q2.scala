import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object WordLengthCountgt2 {
  def main(args: Array[String]) = {
    val sc = getSC()
    val counts = doWordLengthCount(sc)
    saveit(counts)
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    sc
  }
  def doWordLengthCount(sc: SparkContext) = {
    val input = sc.textFile("/datasets/wap")
    val words = input.flatMap(_.split(" "))
    val words_filtered = words.filter(_.size > 2)
    val kv = words_filtered.map(word => (word.size,1)) 
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }
  def saveit(counts: org.apache.spark.rdd.RDD[(Int, Int)]) = {
    counts.saveAsTextFile("ScalaLab1_Q2")
  }
}
