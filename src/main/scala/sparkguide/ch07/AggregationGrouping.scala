package sparkguide.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationGrouping {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
    df.cache()
    df.createOrReplaceTempView("dfTable")

    df.groupBy("InvoiceNo", "CustomerId")
        .count()
        .show()

    df.groupBy("InvoiceNo")
        .agg(
          count("Quantity").alias("quan1"),
          expr("count(Quantity) as quan2")
        ).show()

    df.groupBy("InvoiceNo")
        .agg("Quantity"->"avg", "Quantity"->"stddev_pop")
        .show()
  }
}
