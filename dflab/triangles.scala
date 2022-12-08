import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import spark.implicits._

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
    def getTestFB(spark: SparkSession): DataFrame = {
       
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

    def numTriangles(graph: DataFrame):  = {
        val flipped = graph.selectExpr("user2 as user1", "user1 as user2")
        val combined = graph.union(flipped).distinct()

        val midStart = combined.selectExpr("user1 as mid0", "user2 as start")
        val midEnd = combined.selectExpr("user1 as mid1", "user2 as end")
       
        val join_condition = midStart.col("mid0") === midEnd.col("mid1") 
        val midStartEnd = midStart.join(midEnd, join_condition).selectExpr("mid0 as mid","start","end")
 
         //now we remove useless things like ("D", ("A", "A"))
        val cleaned = midStartEnd.where("start != end")
       
        // ("D", ("A", "C")) -- joining edges (D,A) with (D, C) is a path from A to C via D
        // ("A", ("D", "B"))
        // ("A", ("D", "C"))
        // we would know that ADC is a triangle if there was an edge ("A", "C")
        // so the value tuple says if you have an edge that looks like the value,
        // then we have a triangle
        //
        // "combined" rdd has the list of edges, that is what we need to check
        // so we need to check whether ("A", "C") is in combined, whether ("D", "B") etc.
        val triangle_condition = midStartEnd.col("start") === combined.col("user1") &&
                                 midStartEnd.col("end") === combined.col("user2")
        val all_joined = cleaned.join(combined,triangle_condition)
        
        //     (("A", "C"), ("D", 1))  -- there is a path from A to D to C, and also edge AC
        //                                so that is a triangle
        // but there are 6 ways to represent the same triangle and all are here:
        //     (("D", "A"), ("C", 1))
        //     (("D", "C"), ("A", 1))
        //     etc.
        all_joined.count() / 6
 }
 
}
