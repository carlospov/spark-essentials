package part3typesdatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Encode

import java.sql.Date

object DataSets extends App{
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master","local")
    .getOrCreate()

  val numbersDF = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

//  // we would like to do something like
//  numbersDF.filter(_ > 100)
//  // but as we are using dataFrames we HAVE TO do
//  numbersDF.filter(col("numbers") > 100)

  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int] // Dataset[Int] is a distributed collection of ints

  numbersDS.filter(_ < 100) // we can pass every scala expression that we want


  // what if there's more columns?? dataset of a complex type
  // step 1- define your type, most of the time it will be a case class
  case class Car(
                 Name: String,
                 Miles_per_Gallon: Double,
                 Cylinders: Long,
                 Displacement: Double,
                 Horsepower: Long,
                 Weight_in_lbs: Long,
                 Acceleration: Double,
                 Year: Date,
                 Origin: String
                )  // types are as in a schema

  // step 2 - read dataframe
  def readDataFrame(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = spark.read
    .schema(Encoders.product[Car].schema)
    .json(s"src/main/resources/data/cars.json")


  // step 3 - define an encoder, but most of the time it will be solved by importing implicits._
  //implicit val carEncoder = Encoders.product[Car] // .product will take as type arg every type that extends the Product type
                                                  // all case classes do
  // but defining an implicit for every case class is very tedious, so there's something on Spark that helps us: spark.implicits
  import spark.implicits._
  // this imports all encoders we might ever want to use

  // step 4 - create dataset from dataframe
  val carsDS = carsDF.as[Car]


  // DS collection functions
  numbersDS.filter(_ < 100).show





}
