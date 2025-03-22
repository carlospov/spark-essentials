package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct

object Aggregations extends App{

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")

  // COUNTING
  import org.apache.spark.sql.functions.{count, col, count_distinct, approx_count_distinct, min, max, sum, avg, mean, stddev}
  val genresCount = moviesDF.select(count(col("Major_Genre"))) // all the values except nulls
  genresCount.show()

  // other way
  moviesDF.selectExpr("count(Major_Genre)") // count all non-null values from the column "Major_Genre"
  moviesDF.select(count("*")) // count all the rows, including nulls

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))) // to approx num of distinct values when doing big data

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross"))).show()
  moviesDF.selectExpr("sum(US_Gross)").show()

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating"))).show()
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  // std and mean
  moviesDF.select(mean(col("Rotten_Tomatoes_Rating")), stddev(col("Rotten_Tomatoes_Rating"))).show()
  moviesDF.selectExpr("mean(Rotten_Tomatoes_Rating)", "stddev(Rotten_Tomatoes_Rating)").show()


  // GROUPING
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count() // select count(*) from moviesDF group by Major_Genre

  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating","Rotten_Tomatoes_Rating")

  avgRatingByGenreDF.show()

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenreDF.show()


  /**
   * Exercises
   * 1. Sum up all the profits of all the movies in the DF
   * 2. Count how many distinct directors are in the df
   * 3. Show the mean and std of US gross revenue for the movies
   * 4. compute the avg imdb rating and the avg us_gross revenue per director and sort by any of that
   */

  // 1
  moviesDF.selectExpr("US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross").selectExpr("sum(Total_Gross)").show()

  // 2
  moviesDF.select(countDistinct(col("Director"))).show()

  // 3
  moviesDF.selectExpr("avg(US_Gross)","stddev(US_Gross)").show()

  // 4
  moviesDF
    .groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Rating"),
      avg("US_Gross").as("Profit")
    )
    .orderBy(col("Profit").desc_nulls_last)
    .show()








}
