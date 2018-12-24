package sparkguide.ch06

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object DataTypeString {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/retail-data/by-day/2010-12-01.csv")

    df.createOrReplaceTempView("dfTable")

    changeCase()
    trimming()
    padding()
    replaceRegex()
    translateChars()
    extractRegex()
    containsString()

    def changeCase(): Unit = {
      df.select(
        col("Description"),
        initcap(col("Description")),
        lower(col("Description")),
        upper(col("Description"))
      ).show(2, false)

      session.sql("SELECT initcap(Description) FROM dfTable").show(2, false)
    }

    def trimming(): Unit = {
      df.select(
        ltrim(lit("   HELLO   ")).as("ltrim"),
        rtrim(lit("   HELLO   ")).as("rtrim"),
        trim(lit("   HELLO   ")).as("trim")
      ).limit(1).show()
    }

    def padding(): Unit = {
      df.select(
        lpad(lit("2"), 4, "0"),
        rpad(lit("2"), 4, " ")
      ).limit(1).show()
    }

    def replaceRegex(): Unit = {
      val regex = "BLACK|WHITE|RED|GREEN|BLUE"
      df.select(
        col("Description"),
        regexp_replace(col("Description"), regex, "COLOR").alias("color_replaced")
      ).show(2)

      session.sql("SELECT regexp_replace(Description, '" + regex + "', 'COLOR') FROM dfTable").show(2, false)
    }

    def translateChars(): Unit = {
      // L->1, E->3, T-> 7
      df.select(translate(col("Description"), "LET", "137"), col("Description"))
          .show(2)
    }

    def extractRegex(): Unit = {
      val regex = "(BLACK|WHITE|RED|GREEN|BLUE)"
      df.select(
        col("Description"),
        regexp_extract(col("Description"), regex, 1).alias("color_extracted")
      ).show(2)
    }

    def containsString(): Unit = {
      val containsBlack = col("Description").contains("BLACK")
      val containsWhite = col("Description").contains("WHITE")
      df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
          .where("hasSimpleColor")
          .select("Description").show(3, false)

      session.sql("SELECT Description FROM dfTable WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1").show(3, false)
    }
  }
}
