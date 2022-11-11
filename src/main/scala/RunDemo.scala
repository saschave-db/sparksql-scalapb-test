package myexample

import com.example.protos.demo._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scalapb.spark.ProtoSQL
import java.io.FileOutputStream
import java.io.FileInputStream

object RunDemo {

  def main(Args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._
    import scalapb.spark.Implicits._

    // write
    val path = "/tmp/proto_contacts"
    val op = new FileOutputStream(path)
    testData(0).writeDelimitedTo(op)
    op.flush()
    op.close()

    // read in
    val input = new FileInputStream(path)
    val contact = Contact.parseDelimitedFrom(input).get
    input.close()

    val spark = SparkSession
      .builder()
      .appName("ScalaPB Demo")
      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext

    val personsDF: DataFrame = ProtoSQL.createDataFrame(spark, Seq(contact))
    personsDF.show(false)

    val df_ex = personsDF
      .select(explode(col("people")).as("expl"))
      .select(col("expl.*"))

    df_ex.printSchema()
    df_ex.show(false)
  }

  val testData: Seq[Contact] = Seq(
    Contact().addPeople(
      Person().update(_.name := "Joe", _.age := 32, _.gender := Gender.MALE),
      Person().update(
        _.name := "Mark",
        _.age := 21,
        _.gender := Gender.MALE,
        _.addresses := Seq(
          Address(city = Some("San Francisco"), street = Some("3rd Street"))
        )
      ),
      Person().update(
        _.name := "Steven",
        _.gender := Gender.MALE,
        _.addresses := Seq(
          Address(city = Some("San Francisco"), street = Some("5th Street")),
          Address(city = Some("Sunnyvale"), street = Some("Wolfe"))
        )
      ),
      Person().update(_.name := "Batya", _.age := 11, _.gender := Gender.FEMALE)
    )
  )
}
