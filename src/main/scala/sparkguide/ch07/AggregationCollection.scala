package sparkguide.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_set, collect_list}

object AggregationCollection {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
    df.cache()
    df.createOrReplaceTempView("dfTable")

    df.agg(
      collect_set("Country"),
      collect_list("Country")
    ).show()
  }
}
