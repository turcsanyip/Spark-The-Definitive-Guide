package sparkguide.ch08

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object VariousJoinTricks {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val testData = TestData(session)
    val person = testData.person
    val graduateProgram = testData.graduateProgram
    val sparkStatus = testData.sparkStatus

    arrayContains()

    def arrayContains(): Unit = {
      person.withColumnRenamed("id", "personId")
          .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

      person.join(sparkStatus, array_contains(
        person.col("spark_status"),
        sparkStatus.col("id"))
      ).show()

      session.sql(
        """
          |SELECT *
          |FROM person p
          |JOIN sparkStatus s ON array_contains(p.spark_status, s.id)
        """.stripMargin)
          .show()
    }
  }
}
