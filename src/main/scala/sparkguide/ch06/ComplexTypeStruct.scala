package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}

object ComplexTypeStruct {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/retail-data/by-day/2010-12-01.csv")

    df.selectExpr("(Description, InvoiceNo) as complex", "*").show(false)
    df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show(false)

    val cdf = df.select(struct("Description", "InvoiceNo").alias("complex"))
    cdf.show(false)
    cdf.createOrReplaceTempView("dfTable")

    cdf.select("complex.Description").show(false)
    cdf.select(col("complex").getField("Description")).show(false)

    cdf.select("complex.*").show(false)

    session.sql("SELECT complex.* FROM dfTable").show(false)
  }
}
