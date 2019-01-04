package sparkguide.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AggregationWindow {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
    df.cache()

    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")
    //  dfWithDate.show()

    val window = Window
        .partitionBy("CustomerId", "date")
        .orderBy(col("Quantity").desc)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
        .select(
          col("CustomerId"),
          col("date"),
          col("Quantity"),
          max(col("Quantity")).over(window).alias("maxPurchaseQuantity"),
          dense_rank().over(window).alias("quantityRank"),
          rank().over(window).alias("quantityDenseRank")
        ).show()
  }
}
