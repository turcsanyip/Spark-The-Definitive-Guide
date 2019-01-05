package sparkguide.ch08

import org.apache.spark.sql.SparkSession

object NaturalJoin {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    session.sql(
      """
        |SELECT *
        |FROM person
        |NATURAL JOIN graduateProgram
        |""".stripMargin)
        .show()
  }
}
