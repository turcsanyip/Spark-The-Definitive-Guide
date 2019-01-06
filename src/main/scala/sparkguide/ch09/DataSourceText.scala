package sparkguide.ch09

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataSourceText {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()

    val df = session.read
        .format("text")
        .load("data/flight-data/csv/2010-summary.csv")

    df.show(5)

    df.write
        .format("text")
        .mode("overwrite")
        .save("tmp/my-txt-file.txt")
  }
}
