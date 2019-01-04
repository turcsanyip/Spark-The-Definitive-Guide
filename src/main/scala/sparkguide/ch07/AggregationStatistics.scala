package sparkguide.ch07

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationStatistics {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
    df.cache()
    df.createOrReplaceTempView("dfTable")

    varianceAndStandardDeviation()
    skewnessAndKurtosis()
    covarianceAndCorrelation()

    def varianceAndStandardDeviation(): Unit = {
      // variance: the average of the squared differences from the mean
      // standard deviation: the square root of the variance
      df.select(
        variance("Quantity"), // == var_samp
        var_samp("Quantity"),
        var_pop("Quantity"),
        stddev("Quantity"), // == stddev_samp
        stddev_samp("Quantity"),
        stddev_pop("Quantity")
      ).show()
    }

    def skewnessAndKurtosis(): Unit = {
      // skewness: measures the asymmetry of the values in your data around the mean
      // kurtosis: a measure of the tail of data
      df.select(skewness("Quantity"), kurtosis("Quantity")).show()
    }

    def covarianceAndCorrelation(): Unit = {
      df.select(
        covar_samp("InvoiceNo", "Quantity"),
        covar_pop("InvoiceNo", "Quantity"),
        corr("InvoiceNo", "Quantity")
      ).show()
    }
  }
}
