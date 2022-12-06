// Put your import statements here
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import session.implicits._

object DFlab {
    def main(Args: Array[String]) = {
        val spark = getSS()
        //val df = getFB(spark)
        val df = getTestFB(spark)


  }
    def getSS() = {
        val session = SparkSession.builder.getOrCreate()
        session
  }

    def getFB(spark: SparkSession): DataFrame = {
       val data_location = "/datasets/facebook/"
       val mySchema = new StructType().add("user1", StringType, true)
                                      .add("user2", StringType, true)
       val df = spark.read.format("csv")
                .schema(mySchema)
                .option("delimiter", " ")
                .load(data_location)    
  }
    def getTestFB(spark: SparkSession)" DataFrame = {
       
        val df = List(
                   ("A", "B"),
                   ("B", "A"),
                   ("A", "C"),
                   ("B", "C"),
                   ("A", "D"),
                   ("C", "E"),
                   ("D", "C"),
                   ("C", "A"),
                   ("C", "B"),
                   ("D", "A"),
                   ("C", "D"),
                   ("E", "C")
        ).toDF("user1","user2")

        //   A    ------   B
        //   |    -        |
        //   |      -      |
        //   D    -----    C ------ E
        //
        // 2 triangles: ABC, ADC

        df
  }

    def numTriangles(graph: DataFrame) = {
        val flipped = graph.selectExpr("user2 as user3", "user1 as user4")
        val combined = graph.union(flipped).distinct()
       
        val join_condition = combined.col("user1") === combined.col("user3") 
        val selfjoin = combined.join(combined, join_condition).drop("user3")
  }
 
    //create a test rdd, put in the correct types and create a datafrane
    def getTestDataFrame(spark) = {
      List((1,2,3), (4,5,6)).toDF("a", "b", "c")
   }

}
~
