package sparkguide.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationBasics {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
    df.cache()
    df.createOrReplaceTempView("dfTable")

    val cnt = df.count()
    println(cnt)

    counts()
    firstLast()
    minMax()
    sums()
    avgs()

    def counts(): Unit = {
      df.select(count("StockCode")).show()

      df.select(countDistinct("StockCode")).show()

      df.select(approx_count_distinct("StockCode", 0.1)).show()
    }

    def firstLast(): Unit = {
      df.select(first("StockCode"), last("StockCode")).show()
    }

    def minMax(): Unit = {
      df.select(min("Quantity"), max("Quantity")).show()
    }

    def sums(): Unit = {
      df.select(sum("Quantity")).show()

      df.select(sumDistinct("Quantity")).show()
    }

    def avgs(): Unit = {
      df.select(avg("Quantity")).show()
      df.select(mean("Quantity")).show()
    }
  }
}
