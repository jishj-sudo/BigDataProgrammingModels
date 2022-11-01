import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
object AmountSpentPerCountry {
  def main(args: Array[String]) = {
    val sc = getSC()
    val amountPerCountry = doAmountSpentPerCountry(sc)
    saveit(amountPerCountry)
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    sc
 }
  def doAmountSpentPerCountry(sc: SparkContext) = {
    val input = sc.textFile("/datasets/retailtab")
    val splitInput = input.map(_.split("\\t")).filter(x => x(3) != "Quantity")
    val simplifiedInput = splitInput.map(x => (x(7).toString, x(3).toDouble * x(6).toDouble))
    val amountPerCountry = simplifiedInput.reduceByKey((x,y) => x+y)
    amountPerCountry
  }
  def saveit(amountPerCountry: org.apache.spark.rdd.RDD[(String, Double)]) = {
    amountPerCountry.saveAsTextFile("sparklab-q3")
  }
}
