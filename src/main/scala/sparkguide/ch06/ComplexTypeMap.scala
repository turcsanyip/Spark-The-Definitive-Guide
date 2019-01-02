package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, map}

object ComplexTypeMap {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/retail-data/by-day/2010-12-01.csv")

    df.select(map(col("Description"), col("InvoiceNo")).alias("map")).show(2, false)
  }
}
