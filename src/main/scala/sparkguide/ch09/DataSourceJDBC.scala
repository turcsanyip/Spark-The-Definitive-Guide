package sparkguide.ch09

import java.util.Properties

import org.apache.spark.sql.SparkSession

object DataSourceJDBC {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[1]").getOrCreate()

    val driver = "org.sqlite.JDBC"
    val path = "data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:${path}"
    val tablename = "flight_info"

    testConnection(url)

    val df = session.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", tablename)
        .load()

    df.show(5)

    df.write
        .format("jdbc")
        .mode("overwrite")
        .option("driver", driver)
        .option("url", "jdbc:sqlite:tmp/my-sqlite.db")
        .option("dbtable", tablename)
        .save()


    //    dbAccess()
    //
    //    queryPushdown()
    //
    //    partitions()

    def dbAccess(): Unit = {
      val df = session.read
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("dbtable", tablename)
          .load() // db access

      //df.cache()

      df.show(5) // db access

      df.printSchema() // no db access

      df.select("DEST_COUNTRY_NAME").distinct().explain() // no db access

      df.show(5) // db access if no cache

      df.where("DEST_COUNTRY_NAME = 'United States'").show(5) // db access if no cache

      df.where("DEST_COUNTRY_NAME = 'United States'").explain() // query pushdown if no cache
    }

    def queryPushdown(): Unit = {
      val df = session.read
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("dbtable", "(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info")
          .load()

      df.explain()
      df.show(5)
    }

    def partitions(): Unit = {
      val df = session.read
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("dbtable", tablename)
          .option("numPartitions", 4).load()
      println(df.rdd.getNumPartitions) // 1

      val props = new Properties
      props.setProperty("driver", "org.sqlite.JDBC")

      val predicates = Array(
        "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
        "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
      val df2 = session.read.jdbc(url, tablename, predicates, props)
      df2.show()
      println(df2.rdd.getNumPartitions) // 2

      val colName = "count"
      val lowerBound = 0L
      val upperBound = 348113L // this is the max count in our database
      val numPartitions = 10
      val df3 = session.read.jdbc(url, tablename, colName, lowerBound, upperBound, numPartitions, props)
      df3.show()
      println(df3.rdd.getNumPartitions)
    }
  }

  def testConnection(url: String): Unit = {
    import java.sql.DriverManager
    val connection = DriverManager.getConnection(url)
    connection.isClosed()
    connection.close()
  }
}
