package sparkguide.ch08

import org.apache.spark.sql.SparkSession

object CrossJoin {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val testData = TestData(session)
    val person = testData.person
    val graduateProgram = testData.graduateProgram

    person.crossJoin(graduateProgram).show()

    session.sql(
      """
        |SELECT *
        |FROM person
        |CROSS JOIN graduateProgram
        |""".stripMargin)
        .show()
  }
}
