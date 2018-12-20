package sparkguide.ch02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.max

object SQLExamples {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val flightData2015 = session
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("data/flight-data/csv/2015-summary.csv")

    flightData2015.createOrReplaceTempView("flight_data_2015")

    println("\nSelect max flights:")
    session
        .sql("SELECT max(count) FROM flight_data_2015")
        .show()

    val maxFlights = flightData2015
        .select(max("count"))
        .take(1)(0)
    println(maxFlights) // Row

    println("\nSelect top destination countries:")
    session.sql(
      """
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5
        """).show()

    val topDestinationCountries = flightData2015
        .groupBy("DEST_COUNTRY_NAME")
        .sum("count")
        .withColumnRenamed("sum(count)", "destination_total")
        .orderBy(desc("destination_total"))
        .limit(5)
    topDestinationCountries.show()
    topDestinationCountries.explain(true)
  }
}
