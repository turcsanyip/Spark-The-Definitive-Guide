package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, get_json_object, json_tuple, to_json}

object JSONData {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val jsonDF = session.range(1).selectExpr(
      """
       '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

    jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")).show(false)


    val df = session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/retail-data/by-day/2010-12-01.csv")

    df.createOrReplaceTempView("dfTable")

    df.selectExpr("(Description, InvoiceNo) as complex", "*")
        .select(to_json(col("complex")))
        .show(false)
  }
}
