package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, expr, size, split}

object ComplexTypeArray {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/retail-data/by-day/2010-12-01.csv")

    df.createOrReplaceTempView("dfTable")

    df.select(split(col("Description"), " ")).show(2, false)

    session.sql("SELECT split(Description, ' ') FROM dfTable").show(2, false)

    df.select(split(col("Description"), " ").alias("arr"))
        .select(size(col("arr")), expr("arr[0]"))
        .show(2, false)

    df.select(explode(split(col("Description"), " "))).show(false)
  }
}
