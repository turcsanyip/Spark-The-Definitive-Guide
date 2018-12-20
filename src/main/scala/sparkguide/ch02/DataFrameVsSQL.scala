package sparkguide.ch02

import org.apache.spark.sql.SparkSession

object DataFrameVsSQL {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()

    val flightData2015 = session
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("data/flight-data/csv/2015-summary.csv")


    val dataFrameWay = flightData2015
        .groupBy("DEST_COUNTRY_NAME")
        .count()

    dataFrameWay.explain()


    // convert DataFrame to "view" (which is also a DataFame)
    flightData2015.createOrReplaceTempView("flight_data_2015")

    val sqlWay = session.sql(
      """
        SELECT DEST_COUNTRY_NAME, count(1)
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        """)

    sqlWay.explain()
    //sqlWay.show()
  }
}
