package sparkguide.ch08

import org.apache.spark.sql.{DataFrame, SparkSession}

case class TestData(person: DataFrame, graduateProgram: DataFrame, sparkStatus: DataFrame)

object TestData {

  def apply(session: SparkSession): TestData = {
    import session.implicits._ // toDF() comes with this !!

    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100))
    ).toDF("id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley")
    ).toDF("id", "degree", "department", "school")

    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor")
    ).toDF("id", "status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    new TestData(person, graduateProgram, sparkStatus)
  }

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val testData = TestData(session)
    val person = testData.person
    val graduateProgram = testData.graduateProgram
    val sparkStatus = testData.sparkStatus

    person.show()
    graduateProgram.show()
    sparkStatus.show()
  }
}
