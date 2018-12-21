package sparkguide.ch05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit}

object DataFrameColumnTransformations {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.json("data/flight-data/json/2015-summary.json")
    df.createOrReplaceTempView("dfTable")

    columnReference()
    columnAlias()
    addColumnConstant()
    addColumnCalculated()
    dropColumn()
    castColumnType()

    def columnReference() {
      // col(), column(), expr(), df.col() can be mixed, string not
      df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)
      session.sql("SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2").show()
    }

    def columnAlias() {
      df.select(col("DEST_COUNTRY_NAME").alias("destination")).show(2)
      df.select(expr("DEST_COUNTRY_NAME as destination")).show(2)
      session.sql("SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2").show()
    }

    def addColumnConstant() {
      // add column with constant value
      df.select(col("DEST_COUNTRY_NAME"), lit(1).alias("one")).show(2)
      df.select("DEST_COUNTRY_NAME").withColumn("one", lit(1)).show(2)
      session.sql("SELECT DEST_COUNTRY_NAME, 1 as one FROM dfTable LIMIT 2").show()
    }

    def addColumnCalculated() {
      // add column with calculated value
      df.select(col("DEST_COUNTRY_NAME"), col("ORIGIN_COUNTRY_NAME"),
        expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME").alias("withinCountry")).show(2)
      df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
          .withColumn("withinCountry", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")).show(2)
      session.sql("SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as DEST_COUNTRY_NAME FROM dfTable LIMIT 2").show()
    }

    def dropColumn() {
      df.drop("count").show(2)
    }

    def castColumnType() {
      df.withColumn("count2", col("count").cast("long")).show(2)
      session.sql("SELECT *, cast(count as long) as count2 FROM dfTable LIMIT 2").show()
    }
  }
}
