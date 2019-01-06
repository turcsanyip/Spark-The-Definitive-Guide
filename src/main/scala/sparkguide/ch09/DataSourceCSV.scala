package sparkguide.ch09

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataSourceCSV {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()

    val mySchema = new StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false)
    ))

    val df = session.read
        .format("csv")
        .option("header", "true")
        .option("mode", "failfast") // dropMalformed, permissive
        .schema(mySchema)
        .load("data/flight-data/csv/2010-summary.csv")

    df.show(5)

    df.write
        .format("csv")
        .mode("overwrite")
        .option("sep", "\t")
        .save("tmp/my-tsv-file.tsv")
  }
}
