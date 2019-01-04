package sparkguide.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationRollup {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
        .withColumn("date", to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
        .na.drop() // remove null values

    df.rollup("Date", "Country")
        .agg(sum("Quantity"))
        .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
        //.where(col("Country").isNotNull) // only subtotals for each country on each date
        //.where(col("Country").isNull) // only grand totals for each date
        //.where("Date IS NULL") // only grand total over all dates
        .orderBy(col("Date").asc_nulls_last, col("Country").asc_nulls_last)
        .show()
  }
}
