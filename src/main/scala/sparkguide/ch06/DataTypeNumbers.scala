package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataTypeNumbers {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/retail-data/by-day/2010-12-01.csv")

    df.createOrReplaceTempView("dfTable")

    rounding()
    correlation()
    statistics()

    def rounding(): Unit = {
      df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

      df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
      session.sql("SELECT round(2.5), bround(2.5)").show()
    }

    def correlation(): Unit = {
      df.select(corr("Quantity", "UnitPrice")).show()
      session.sql("SELECT corr(Quantity, UnitPrice) FROM dfTable").show()
    }

    def statistics(): Unit = {
      // count, mean, stddev, min, max => functions too
      df.describe().show()
    }
  }
}
