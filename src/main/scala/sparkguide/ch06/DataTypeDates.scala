package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataTypeDates {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val dateDF = session.range(1)
        .withColumn("today", current_date())
        .withColumn("now", current_timestamp())
    dateDF.createOrReplaceTempView("dateTable")

    dateDF.printSchema()
    dateDF.show(false)

    addDays()
    diffDates()
    dateLiteral()
    timestampLiteral()

    def addDays() {
      dateDF.select("today")
          .withColumn("-5 days", date_sub(col("today"), 5))
          .withColumn("+5 days", date_add(col("today"), 5))
          .show(false)

      session.sql("SELECT today, date_sub(today, 5), date_add(today, 5) FROM dateTable").show(false)
    }

    def diffDates(): Unit = {
      dateDF.withColumn("week_ago", date_sub(col("today"), 7))
          .select(
            col("today"),
            col("week_ago"),
            datediff(col("week_ago"), col("today")).alias("diff"))
          .show(1)

      dateDF.select(
        to_date(lit("2016-01-01")).alias("start"),
        to_date(lit("2017-05-22")).alias("end"))
          .select(months_between(col("start"), col("end"))).show(1)

      session
          .sql("SELECT to_date('2016-01-01'), months_between('2016-01-01', '2017-01-01'), datediff('2016-01-01', '2017-01-01') FROM dateTable")
          .show()
    }

    def dateLiteral(): Unit = {
      // format string is optional
      // unparsable value will be null
      dateDF.select(
        to_date(lit("2018-12-23")),
        to_date(lit("not a date")))
          .show()

      val dateFormat = "yyyy.MM.dd"
      dateDF.select(
        to_date(lit("2018.12.23"), dateFormat))
          .show()
    }

    def timestampLiteral(): Unit = {
      dateDF
          .select(
            to_timestamp(lit("2018-12-24 00:12:00"))
          )
          .show()
    }
  }
}
