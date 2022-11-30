import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Q3 { //remove extends APP
  def main(args: Array[String]) = {
    // call your code using spark-submit nameofjarfile.jar commandlinearg
    val thresholdPopular = args(0).toFloat // this is the commandlinearg
    val thresholdHub = args(1).toFloat
    val sc = getSC()
    val input = getRDD(sc)
    //val input = getTestRDD(sc)
    val popular = findPopular(input, thresholdPopular)
    val hubs = findHubs(input, thresholdHub)
    val popularAndHub = popular.join(hubs)
    saveit(popularAndHub)
  }

  def getSC() = {
    val conf = new SparkConf().setAppName("popularAndHub")
    val sc = new SparkContext(conf)
    sc
  }
  def getRDD(sc: SparkContext) = {
    val input = sc.textFile("/datasets/flight")
    input 
  }

  

  //incoming - outgoing >= threshold
   def findPopular(input: RDD[String], threshold: Float) = {
    val inputSplit = input.map(_.split(",")).filter(flight => flight(0) != "ITIN_ID")

    //(ITIN_ID, YEAR, QUARTER, ORIGIN, ORIGIN_STATE_NM, DEST, DEST_STATE_NM, PASSENGERS)
    val kv = inputSplit.flatMap(flight => List( (flight(3), (0.toFloat, flight(7).toFloat)) ,(flight(5),(flight(7).toFloat,0.toFloat))))
    val kvReduced = kv.aggregateByKey((0.toFloat,0.toFloat))(((x,y) => (x._1 +y._1, x._2+y._2)), ((x,y) => (x._1+y._1, x._2+y._2)))
    val popular = kvReduced.filter{case (airport, (incoming, outgoing)) => (incoming-outgoing) >= threshold}
    popular
  }
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
  def saveit(popularAndHub: RDD[(String,((Float,Float),Int))]) = { 
    popularAndHub.saveAsTextFile("sparkhw-q3")
  }
}
