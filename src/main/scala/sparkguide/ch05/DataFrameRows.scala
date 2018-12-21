package sparkguide.ch05

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameRows {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val myRow = Row("Hello", null, 1, false)

    println(myRow(0)) // Any
    println(myRow(0).asInstanceOf[String])
    println(myRow.getString(0))
    println(myRow.getAs[String](0))

    val mySchema = StructType(Array(
      StructField("col1", StringType, true),
      StructField("col2", StringType, true),
      StructField("col3", IntegerType, true),
      StructField("col4", BooleanType, true)
    ))
    val myRdd = session.sparkContext.parallelize(List(myRow))
    val myDf = session.createDataFrame(myRdd, mySchema)
    myDf.show()
  }
}
