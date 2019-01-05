package sparkguide.ch08

import org.apache.spark.sql.SparkSession

object LeftAntiJoin {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val testData = TestData(session)
    val person = testData.person
    val graduateProgram = testData.graduateProgram

    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    graduateProgram.join(person, joinExpression, "left_anti").show() // alt: leftanti

    session.sql(
      """
        |SELECT *
        |FROM graduateProgram
        |LEFT ANTI JOIN person ON person.graduate_program = graduateProgram.id
        |""".stripMargin)
        .show()
  }
}
