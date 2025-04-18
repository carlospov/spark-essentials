package part5lowlevel

import org.apache.spark.sql.SparkSession

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
  case class StockValue(company: String, date: String, price: Double)
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



}
