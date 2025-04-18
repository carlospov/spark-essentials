package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App{
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext // to operate with RDDs

  // Creating RDDs
  // 1 - paralellize an existing collection
  val numbers = 1 to 100000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1) // to drop header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0)) // to filter header (knowing how the header is, in this case we know is lowercase)
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DataFrame
  val stocksDF = spark.read
    .option("header","true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksRDD4 = stocksDF.rdd // also valid, but we will obtain RDD[ROw], not RDD[StockValue]

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd // RDD[StockValue]
  // ^^do this to keep type information

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type information because DF have no types

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // this keeps type information

  // lesson 2 on RDDs

  // Transformations
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION

  // counting
  val campanyNamesRDD = stocksRDD.map(_.symbol).distinct() // also lazy

  // min and max

  // we have to define an ordering, any following three ways will work
//  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
//  implicit val stockOrdering = Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  implicit val stockOrdering = Ordering.fromLessThan((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping ( groupBy is overloaded with number of partitions or partitioner
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // caution, grouping is very expensive bc it involves shuffles

  // Partitioning (involves shuffle also)
  val repartitionedStocksRDD = stocksRDD.repartition(30) // we get a new RDD of StockValue
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30") // will create one parquet file (ore one part of a parquet file) for each partition

  /*
  Repartitioning is expensive since it involves shuffle
  Best practice:
    - partition EARLY, then process that partitioned
    - size of each partition should be between 10-100MB for best performance
   */

  // Coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling until explicitly set
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15") // will create 15 files

  /**
   * Exercises (using RDDs transformations)
   *
   * 1. Read movies.json as an RDD.
   * 2. Show the distinct genres as an RDD
   * 3. Select all the movies in Drama genre with IMDB rating > 6
   * 4. Show the avg rating of movies by genre
   */

  // 1
  case class Movie(title: String, genre: String, rating: Double)
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val distinctGenres = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramas = moviesRDD.filter(_.genre == "Drama").filter(_.rating > 6)

  // 4
  val avgRatings = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => (genre, movies.map(_.rating).sum / movies.size)
  }

  moviesRDD.toDF.show()
  distinctGenres.toDF.show()
  goodDramas.toDF.show()
  avgRatings.toDF.show()
}
