package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App{

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show() // <-- using lit

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie")) // creating a boolean column if filters matches
  // filter on a boolean column name
  moviesWithGoodnessFlagsDF.where("good_movie") // where (col("good_movie) === "true")

  // negation
  moviesWithGoodnessFlagsDF.where(not(col("good_movie"))) // where (col("good_movie) === "true")

  // Numbers

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation: number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an ACTION */)

  // strings
  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // more powerful: regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  // substitution
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).show()

  /**
   * Exercise
   * - Filter on a list of car names obtained by an API call
   * -
   */

  def getCarNames: List[String] = List("Volkswagen","Mercedes-Benz","Ford")

  val regex_car_names = getCarNames.map(_.toLowerCase()).mkString("|")
  val filteredCarsDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regex_car_names, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  filteredCarsDF.show()

  // version 2: contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name") contains name)
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show()

}
