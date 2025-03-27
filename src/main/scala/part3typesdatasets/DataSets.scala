package part3typesdatasets

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Encode
import org.apache.spark.sql.functions.{array_contains, avg, col}

import java.sql.Date

object DataSets extends App{
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master","local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read.format("csv")
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
//  case class Car(
//                 Name: String,
//                 Miles_per_Gallon: Double,
//                 Cylinders: Long,
//                 Displacement: Double,
//                 Horsepower: Long,
//                 Weight_in_lbs: Long,
//                 Acceleration: Double,
//                 Year: Date,
//                 Origin: String
//                )  // types are as in a schema
  // ^^^^non-nullable old version

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

  // maps, flatmap, folds, reduce, for comprehensions, filters, everything... on the datasets of our objects
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  carNamesDS.show() // this will action and program will crash because of nulls, that is because of the type of columns
  // typing columns the way we did forces them to be non-nullable

  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )  // this way makes them nullable


  /**
   * Exercises
   * - Count how many cars we have
   * - Count how many powerful (HP > 140) cars we have
   * - Compute the average horsepower for the entire dataset
   */

  val numberOfCars = carsDS.count()
  println(numberOfCars)

  val PowerfulCars = carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()
  println(PowerfulCars)

  val avgHP = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / numberOfCars
  println(avgHP)

  carsDS.select(avg(col("Horsepower"))).show()  // same as above, we can also use the DF functions on the DS


  // datasets Part 2

  // Joins
  case class Guitar(
                   id: Long,  // Long instead of Int because when we are inferring Schema, Spark reads numbers as Longs by default
                   make: String,
                   model: String,
                   guitarType: String
                   )

  case class GuitarPlayer(
                         id: Long,
                         name: String,
                         guitars: Seq[Long],
                         band: Long
                         )

  case class Band(
                 id: Long,
                 name: String,
                 hometown: String,
                 year: Long
                 )

  val guitarsDS = readDataFrame("guitars.json").as[Guitar]
  val guitarPlayersDS = readDataFrame("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDataFrame("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayerBandsDS.show()

//  +--------------------+--------------------+
//  |                  _1|                  _2|
//  +--------------------+--------------------+
//  |{1, [1], 1, Angus...|{Sydney, 1, AC/DC...|
//  |{0, [0], 0, Jimmy...|{London, 0, Led Z...|
//  |{3, [3], 3, Kirk ...|{Los Angeles, 3, ...|
//  +--------------------+--------------------+
//
//  The columns are _1 and _2 bc we have a dataset of a "tuple of seq of types", so each of the column contains
//  a set of values corresponding to the values in the columns of original datasets
//  We can't change name of columns, they don't actually exist

  /**
   * Exercise
   *
   * - Join GuitarPlayersDS with GuitarsDS with condition guitar id is contained in the guitar player's guitars seq
   *    and use outer join
   *    Hint: Use array_contains
   */

  val exercise1 = guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
  exercise1.show()

  // Grouping
  //  val carsGroupedByOrigin: KeyValueGroupedDataset[String, Car] = carsDS.groupByKey(_.Origin)
  // val carsGroupedByOrigin: Dataset[(String, Long)] = carsDS.groupByKey(_.Origin).count()
  val carsGroupedByOrigin= carsDS.groupByKey(_.Origin).count().show()













  // notes:

//  in Spark 3.0.2 the cars dataFrame cant be read:
//    Cannot up cast `Year` from string to date.
//  The type path of the target object is:
//  - field (class: "java.sql.Date", name: "Year")
//  - root class: "part3typesdatasets.DataSets.Car"
//  You can either add an explicit cast to the input data or choose a higher precision type of the field in the target object;
//  The way to solve is: add the caseclass's schema when reading:
//    spark.read
//    .option("inferSchema", "true")
//    .schema(Encoders.product[Car].schema)
//    .json(s"src/main/resources/data/$filename")


}
