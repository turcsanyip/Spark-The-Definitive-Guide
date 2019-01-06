package sparkguide.ch09

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataSourceJSON {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()

    val mySchema = new StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false)
    ))

    val df = session.read
        .format("json")
        .option("mode", "FAILFAST")
        .schema(mySchema)
        .load("data/flight-data/json/2010-summary.json")

    df.show(5)

    df.write
        .format("json")
        .mode("overwrite")
        .save("tmp/my-json-file.json")
  }
}
