package sparkguide.ch09

import org.apache.spark.sql.SparkSession

object DataSourceORC {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()

    val df = session.read
        .format("orc")
        .load("data/flight-data/orc/2010-summary.orc")

    df.show(5)

    df.write
        .format("orc")
        .mode("overwrite")
        .save("tmp/my-orc-file.orc")
  }
}
