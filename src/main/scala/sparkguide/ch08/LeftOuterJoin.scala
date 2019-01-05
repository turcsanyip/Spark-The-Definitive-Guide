package sparkguide.ch08

import org.apache.spark.sql.SparkSession

object LeftOuterJoin {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val testData = TestData(session)
    val person = testData.person
    val graduateProgram = testData.graduateProgram

    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    graduateProgram.join(person, joinExpression, "left_outer").show() // alt: leftouter, left

    session.sql(
      """
        |SELECT *
        |FROM graduateProgram
        |LEFT OUTER JOIN person ON person.graduate_program = graduateProgram.id
        |""".stripMargin)
        .show()
    // alt: LEFT JOIN
  }
}
