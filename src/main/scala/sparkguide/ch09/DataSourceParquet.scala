package sparkguide.ch09

import org.apache.spark.sql.SparkSession

object DataSourceParquet {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()

    val df = session.read
        .format("parquet")
        .load("data/flight-data/parquet/2010-summary.parquet")

    df.show(5)

    df.write
        .format("parquet")
        .mode("overwrite")
        .save("tmp/my-parquet-file.parquet")
  }
}
