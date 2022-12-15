import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object FacebookDF {
     val spark = SparkSession.builder.getOrCreate()
     import spark.implicits._

    def main(Args: Array[String]) = {
        val df = getFB(spark)
        //val df = getTestFB(spark)
        val FB_Count = FBCount(df)
        writeDF(FB_Count)


  }

    def getFB(spark: SparkSession): DataFrame = {
       val data_location = "/datasets/facebook/"
       val mySchema = new StructType().add("user1", StringType, true)
                                      .add("user2", StringType, true)
       val df = spark.read.format("csv")
                .schema(mySchema)
                .option("delimiter", " ")
                .load(data_location) 
       df   
  }

    def writeDF(df: DataFrame) = {
        df.write.format("csv")
                   .option("header","true")
                   .save("FacebookCountFinal")
  }
    def getTestFB(spark: SparkSession): DataFrame = {
       
        val df = List(
                   ("123", "455"),
                   ("1231", "1232"),
                   ("122", "400"),
                   ("123", "501"),
                   ("123", "502")
        ).toDF("user1","user2")

        df
  }

    def FBCount(facebookDF: DataFrame): DataFrame  = {
        
        val filtered = facebookDF.where("user2 > 500")
        val grouped = filtered.groupBy("user1")
        val agg = grouped.agg(count("user2"))

        agg
 }
 
}
