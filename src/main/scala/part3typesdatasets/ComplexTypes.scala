package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, _}

object ComplexTypes extends App{
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // difference, but there's other functions: date_Add, date_sub

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show() // there's some. Because some dates are on different formatting that we're trying to parse

  /**
   * Exercise
   * 1. How do we deal with multiple date formats
   * 2. Read stocks DF and parse the dates
   */

  // 1 - try to parse 1 and then if nulls, parse nulls with another format, if still nulls....etc then union the small DFs
    // tradeoff: dataset is

  // 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header","true")
    .option("sep",",")
    .option("nullValue","")
    .csv("src/main/resources/data/stocks.csv")

  val stocksWithParsedDate = stocksDF.withColumn("actual_date", to_date(col("date"), "MMM d yyyy"))

  stocksWithParsedDate.show()

  // structures
  // 1- with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
    .show()

  // 2- with expression strings
  moviesDF.selectExpr("Title","(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title","Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()


}
