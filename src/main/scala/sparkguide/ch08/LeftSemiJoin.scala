package sparkguide.ch08

import org.apache.spark.sql.SparkSession

object LeftSemiJoin {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val testData = TestData(session)
    val person = testData.person
    val graduateProgram = testData.graduateProgram

    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    graduateProgram.join(person, joinExpression, "left_semi").show() // alt: leftsemi

    session.sql(
      """
        |SELECT *
        |FROM graduateProgram
        |LEFT SEMI JOIN person ON person.graduate_program = graduateProgram.id
        |""".stripMargin)
        .show()
  }
}
