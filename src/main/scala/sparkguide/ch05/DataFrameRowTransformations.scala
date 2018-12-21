package sparkguide.ch05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, expr}

object DataFrameRowTransformations {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.json("data/flight-data/json/2015-summary.json")
    df.createOrReplaceTempView("dfTable")

    filterRows()
    distinctRows()
    randomSample()
    randomSplit()
    union()
    sorting()
    limit()
    repartitionAndCoalesce()
    collect()

    def filterRows() {
      // filter() == where()
      df.filter(col("count") > 100).show(2)
      df.filter("count > 100").show(2)
      session.sql("SELECT * FROM dfTable WHERE count > 100 LIMIT 2").show()

      df.where("count > 100").where("ORIGIN_COUNTRY_NAME <> 'Ireland'").show(2)
      df.where("count > 100 AND ORIGIN_COUNTRY_NAME <> 'Ireland'").show(2)
      session.sql("SELECT * FROM dfTable WHERE count > 100 AND ORIGIN_COUNTRY_NAME <> 'Ireland' LIMIT 2").show()
    }

    def distinctRows(): Unit = {
      println(df.select("ORIGIN_COUNTRY_NAME").distinct().count())
      session.sql("SELECT count(distinct(ORIGIN_COUNTRY_NAME)) FROM dfTable").show()
    }

    def randomSample(): Unit = {
      df.sample(0.1).show()
    }

    def randomSplit(): Unit = {
      val splits = df.randomSplit(Array(0.25, 0.75), 111)
      println(splits(0).count() + " / " + splits(1).count())
    }

    def union(): Unit = {
      val df2 = session.read.json("data/flight-data/json/2014-summary.json")
      val df3 = df2.union(df)
      println(df.count() + " + " + df2.count() + " = " + df3.count())
    }

    def sorting(): Unit = {
      // sort() == orderBy()
      df.sort("ORIGIN_COUNTRY_NAME").show(5)
      df.sort(desc("ORIGIN_COUNTRY_NAME")).show(5)
      df.sort(col("ORIGIN_COUNTRY_NAME").desc).show(5)
      df.sort(expr("ORIGIN_COUNTRY_NAME desc")).show(5) // does not work ??
      session.sql("SELECT * FROM dfTable ORDER BY ORIGIN_COUNTRY_NAME DESC").show(5)
      // asc_nulls_first, desc_nulls_first, asc_nulls_last, desc_nulls_last
    }

    def limit(): Unit = {
      df.limit(2).show()
      session.sql("SELECT * FROM dfTable LIMIT 2").show()
    }

    def repartitionAndCoalesce(): Unit = {
      println(df.rdd.getNumPartitions)
      val df2 = df.repartition(8)
      println(df2.rdd.getNumPartitions)
      val df3 = df2.repartition(4, col("ORIGIN_COUNTRY_NAME"))
      println(df3.rdd.getNumPartitions)
      val df4 = df3.coalesce(2)
      println(df4.rdd.getNumPartitions)
    }

    def collect(): Unit = {
      // another methods to collect data to the driver: take(), show()
      val result = df.select("ORIGIN_COUNTRY_NAME").distinct().sort().collect()
      println(result.length)
      val iter = df.select("ORIGIN_COUNTRY_NAME").distinct().sort().toLocalIterator()
      for (i <- 0 to 5)
        println(iter.next())
    }
  }
}
