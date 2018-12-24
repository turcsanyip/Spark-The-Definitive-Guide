package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataTypeBoolean {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/retail-data/by-day/2010-12-01.csv")

    //df.printSchema()
    //df.show()
    df.createOrReplaceTempView("dfTable")

    equality()
    andOr()
    addBooleanColumn()

    def equality(): Unit = {
      df.where(col("InvoiceNo").equalTo(536365)) // not eq: notEqual() / not(col().equalTo())
          .select("InvoiceNo", "Description")
          .show(5, false)

      df.where(col("InvoiceNo") === 536365) // not eq: =!=
          .select("InvoiceNo", "Description")
          .show(5, false)

      df.where("InvoiceNo = 536365") // not eq: <> / !=
          .select("InvoiceNo", "Description")
          .show(5, false)
    }

    def andOr(): Unit = {
      val priceFilter = col("UnitPrice") > 600 // gt()
      val descripFilter = col("Description").contains("POSTAGE")
      df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).show()
      df.where(col("StockCode").isin("DOT").and(priceFilter.or(descripFilter))).show()
      df.where(col("StockCode").isin("DOT").and(priceFilter).or(descripFilter)).show() // diff
      session.sql("SELECT * FROM dfTable WHERE StockCode in ('DOT') and (UnitPrice > 600 or Description LIKE '%POSTAGE%')").show()
    }

    def addBooleanColumn(): Unit = {
      val codeFilter = col("StockCode") === "DOT"
      val priceFilter = col("UnitPrice") > 600
      val descripFilter = col("Description").contains("POSTAGE")
      df.withColumn("isExpensive", codeFilter.and(priceFilter.or(descripFilter)))
          .where("isExpensive")
          .select("unitPrice", "isExpensive").show(5)
    }
  }
}
