package sparkguide.ch08

import org.apache.spark.sql.SparkSession

object OuterJoin {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val testData = TestData(session)
    val person = testData.person
    val graduateProgram = testData.graduateProgram

    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpression, "outer").show() // alt: full_outer, fullouter, full

    session.sql(
      """
        |SELECT *
        |FROM person
        |FULL OUTER JOIN graduateProgram ON person.graduate_program = graduateProgram.id
        |""".stripMargin)
        .show()
    // alt: FULL JOIN
  }
}
