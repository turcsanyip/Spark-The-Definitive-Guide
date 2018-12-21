package sparkguide.ch05

import org.apache.spark.sql.SparkSession

object DataFrameSchemaInferred {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    //val df = session.read.format("json").load("data/flight-data/json/2015-summary.json")
    val  df = session.read.json("data/flight-data/json/2015-summary.json")

    df.printSchema()
    println(df.schema)
  }
}
