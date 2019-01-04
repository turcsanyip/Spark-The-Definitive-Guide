package sparkguide.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object AggregationPivoting {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
        .withColumn("date", to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
        .na.drop() // remove null values

    df.groupBy("date")
        .pivot("Country")
        .sum("Quantity")
        .where("date > '2011-12-05'")
        //.select("date" ,"`USA_sum(Quantity)`")
        .show()
  }
}
