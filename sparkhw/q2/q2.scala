import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Q2 { //remove extends APP
  def main(args: Array[String]) = {
    // call your code using spark-submit nameofjarfile.jar commandlinearg
    val threshold = args(0).toFloat // this is the commandlinearg
    val sc = getSC()
    val input = getRDD(sc)
    //val input = getTestRDD(sc)
    val hubs = findHubs(input, threshold)
    saveit(hubs)
  }

  def getSC() = {
    val conf = new SparkConf().setAppName("popular")
    val sc = new SparkContext(conf)
    sc
  }
  def getRDD(sc: SparkContext) = {
    val input = sc.textFile("/datasets/flight")
    input 
  }

  //incoming - outgoing >= threshold
  def findHubs(input: RDD[String], threshold: Float) = {
    val inputSplit = input.map(_.split(",")).filter(flight => flight(0) != "ITIN_ID")
    
    //(ITIN_ID, YEAR, QUARTER, ORIGIN, ORIGIN_STATE_NM, DEST, DEST_STATE_NM, PASSENGERS)
    val kv = inputSplit.map(flight => (flight(3),Set(flight(6))))
    val kvAggregated = kv.aggregateByKey(Set[String]())(((set1,set2) => (set1|set2)), ((set1,set2) => set1|set2))
    val hubs = kvAggregated.map{case (airport, countrySet) => (airport,countrySet.size)}
                           .filter{case (airport, numCountries) => numCountries  >= threshold}
    hubs
  }
  def getTestRDD(sc: SparkContext): RDD[String] = {
    val myTestList = List("ITIN_ID,YEAR,QUARTER,ORIGIN,ORIGIN_STATE_NM,DEST,DEST_STATE_NM,PASSENGERS",
"2021118,2021,1,CAE,South Carolina,FLL,Florida,1.00",
"2021119,2021,1,CAE,South Carolina,FLL,Florida,2.00",
"2021120,2021,1,CAE,South Carolina,FLL,Florida,6.00",
"2021121,2021,1,CAE,South Carolina,FLL,Florida,3.00",
"2021122,2021,1,CAE,South Carolina,FLL,Florida,1.00",
"2021123,2021,1,CAE,South Carolina,FLL,Florida,3.00",
"2021124,2021,1,CAE,South Carolina,FLL,Florida,1.00",
"2021125,2021,1,CAE,South Carolina,FLL,Florida,5.00",
"2021126,2021,1,CAE,South Carolina,FLL,Florida,2.00",
"2021126,2021,1,FLL,Florida,CAE,South Carolina,2.00",
"2021127,2021,1,CAE,South Carolina,FLL,Florida,1.00",
"2021127,2021,1,FLL,Florida,CAE,South Carolina,1.00",
"2021128,2021,1,CAE,South Carolina,FLL,Florida,1.00",
"2021128,2021,1,FLL,Florida,CAE,South Carolina,1.00",
"2021129,2021,1,CAE,South Carolina,FLL,Florida,1.00",
"2021129,2021,1,FLL,Florida,CAE,South Carolina,1.00",
"2021130,2021,1,CAE,South Carolina,FLL,Florida,1.00",
"2021130,2021,1,FLL,Florida,CAE,South Carolina,1.00")

    val input = sc.parallelize(myTestList,2)
    input
  }
  def saveit(popular: RDD[(String,Int)]) = { 
    popular.saveAsTextFile("sparkhw-q2")
  }
}
