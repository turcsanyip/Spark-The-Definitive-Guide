package sparkguide.ch07

import org.apache.spark.sql.SparkSession

object AggregationGroupingSet {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/retail-data/all/*.csv")
        .coalesce(5)
        .na.drop() // remove null values
    df.createOrReplaceTempView("dfTable")

    //df.where("CustomerID IS NULL").show() // should be empty

    session.sql(
      """
        |SELECT CustomerID, StockCode, sum(Quantity)
        |FROM dfTable
        |GROUP BY CustomerID, StockCode
        |GROUPING SETS((CustomerID, StockCode), (CustomerID), ())
        |ORDER BY CustomerID DESC, StockCode DESC
      """.stripMargin)
        //.where("StockCode IS NOT NULL") // only (CustomerID, StockCode) grouping set
        //.where("StockCode IS NULL") // only (CustomerID) grouping set
        //.where("CustomerID IS NULL") // only () grouping set
        .selectExpr("count(*)")
        .show()

    // grouping sets available only in SQL
  }
}
