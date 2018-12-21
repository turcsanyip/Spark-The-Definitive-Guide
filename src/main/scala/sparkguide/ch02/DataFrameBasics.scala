package sparkguide.ch02

import org.apache.spark.sql.SparkSession

object DataFrameBasics {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()
    // 'spark' object in REPL

    val flightData2015 = session
        .read // DataFrameReader
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("data/flight-data/csv/2015-summary.csv") // DataFrame

    flightData2015.printSchema()

    flightData2015.show(10)


    // collect into Array[Row]
    val allRows = flightData2015.collect() // all
    val someRows = flightData2015.take(10) // top 10


    flightData2015.explain()
    flightData2015.sort("count").explain()
  }
}
