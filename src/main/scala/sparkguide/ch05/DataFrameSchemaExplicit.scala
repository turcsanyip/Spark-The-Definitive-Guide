package sparkguide.ch05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataFrameSchemaExplicit {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val myExplicitSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("COUNT", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = session.read.schema(myExplicitSchema).json("data/flight-data/json/2015-summary.json")

    df.printSchema()
    println(df.schema)
  }
}
