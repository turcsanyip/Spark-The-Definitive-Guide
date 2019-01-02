package sparkguide.ch06

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object NullValues {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val schema = StructType(Array(
      StructField("c0", StringType, true),
      StructField("c1", StringType, true),
      StructField("c2", StringType, true),
      StructField("c3", StringType, true)
    ))

    val rows = List(
      Row(null, "v1", "v2", "v3")
    )

    val rdd = session.sparkContext.parallelize(rows)

    val df = session.createDataFrame(rdd, schema)

//    val df = session.range(1)
//        .withColumn("c0", lit(null))
//        .withColumn("c1", lit("v1"))
//        .withColumn("c2", lit("v2"))
//        .withColumn("c3", lit("v3"))

    df.createOrReplaceTempView("nullTable")

    clsc()
    nvls()

    dropRows()
    fillNulls()
    replaceValue()

    def clsc(): Unit = {
      df.select(
        coalesce(col("c0"), col("c1")),
        coalesce(col("c1"), col("c2")),
        coalesce(col("c1"), col("c1"), col("c2"))
      ).show()

      session.sql("SELECT " +
          "coalesce(c0, c1), " +
          "coalesce(c1, c2), " +
          "coalesce(c0, c1, c2) " +
          "FROM nullTable"
      ).show()
    }

    def nvls(): Unit = {
      // = coalesce with 2 args
      session.sql("SELECT " +
          "ifnull(c0, c1), " +
          "ifnull(c1, c2) " +
          "FROM nullTable"
      ).show()

      // returns null, if c1=c2, otherwise c1
      session.sql("SELECT " +
          "nullif(c1, c2), " +
          "nullif(c2, c2) " +
          "FROM nullTable"
      ).show()

      // = ifnull
      session.sql("SELECT " +
          "nvl(c0, c1), " +
          "nvl(c1, c2) " +
          "FROM nullTable"
      ).show()

      // returns c2 if c1 is not null, otherwise c3
      session.sql("SELECT " +
          "nvl2(c0, c1, c2), " +
          "nvl2(c1, c2, c3) " +
          "FROM nullTable"
      ).show()

      // DataFrame expressions: ??
    }

    def dropRows(): Unit = {
      // drops a row if any of its columns contains null
      df.na.drop().show()
      df.na.drop("any").show()

      // drops a row if any of the specified columns contains null
      df.na.drop("any", List("c0", "c1")).show()
      df.na.drop("any", List("c1", "c2")).show()

      // drops a row if all its columns contain null
      df.na.drop("all").show()

      // drops a row if all of the specified columns contain null
      df.na.drop("all", List("c0")).show()
      df.na.drop("all", List("c0", "c1")).show()

      // Spark SQL: only IS NOT NULL
    }

    def fillNulls(): Unit = {
      // only works with String columns => schema must be specified
      df.na.fill("n/a").show()
      df.na.fill("n/a", List("c0")).show()

      df.na.fill(Map("c0" -> "n/a")).show()
    }

    def replaceValue(): Unit = {
      df.na.replace("c1", Map("v1" -> "V1")).show()
    }
  }
}
