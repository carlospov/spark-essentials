package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataframesBasics.spark

object DataSources extends App{

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master","local")
    .getOrCreate()

  val carSchema = StructType(
    Array(
      StructField("Name", StringType, nullable = true),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  /*
  Reading a DataFrame requires the following:
    - format
    - Schema (optional) or inferSchema=true
    - path
    - zero or more options
   */
  // read DF with own Schema
  val carsDF = spark.read
    .format("json")
    .schema(carSchema) // enforce a Schema
    .option("mode","failFast") // dropMalformed, permissive (default) --> how malformed (not matching schema) records are treated
//    .load("src/main/resources/data/cars.json")  // path can be a S3 url
    .option("path","src/main/resources/data/cars.json") // alternative to the above load param
    .load()
  // another way is an option map
  val carsDFwithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true",
    ))
    .load()

  /*
  / Writing DF requires setting:
    - format
    - save mode = overwrite, append, ignore, errorIfExist
    - path
    - zero or more options
   */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")


  // JSON Flags
  spark.read
  //  .format("json")
    .schema(carSchema)
    .option("dateFormat","YYYY-MM-dd") // to parse a String as a DateType given a Schema that considers a DateType if Spark fails that parsing, it will put null
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed") // other values are bzip2, gzip, lz4, snappy, deflate
  // .load("src/main/resources/data/cars.json")
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .format("csv")
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .option("sep",",")
    .option("nullValue","") // "What is a null value?"
    .load("src/main/resources/data/stocks.csv")
    // or .csv and don't specify format

  // PARQUET - default storage format for dataframes
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")
    // or .save("src/main/resources/data/cars.parquet") bc is the default mode

  // Text Files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  employeesDF.show()

  /**
   * Exercise: read the movies DF
   * write it as :
   *  - tab-separated values file
   *  - snappy parquet
   *  - table in the postgres database ("public.movies")
   *
   */

  // 1 read movies
  val moviesSchema = StructType(Array(
  StructField("Title",StringType),
  StructField("US_Gross",LongType),
  StructField("Worldwide_Gross",LongType),
  StructField("US_DVD_Sales",LongType),
  StructField("Production_Budget",LongType),
  StructField("Release_Date",StringType),
  StructField("MPAA_Rating",StringType),
  StructField("Running_Time_min",LongType),
  StructField("Distributor",StringType),
  StructField("Source",StringType),
  StructField("Major_Genre",StringType),
  StructField("Creative_Type",StringType),
  StructField("Director",StringType),
  StructField("Rotten_Tomatoes_Rating",LongType),
  StructField("IMDB_Rating",DoubleType),
  StructField("IMDB_Votes",LongType)
  ))

  val moviesDF = spark.read
    .format("json")
    .schema(moviesSchema)
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed") // other values are bzip2, gzip, lz4, snappy, deflate
    .load("src/main/resources/data/movies.json")

  // write as tsv
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .option("header","true")
    .option("sep","\t")
    .option("nullValue","") // "What is a null value?"
    .save("src/main/resources/data/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.movies")
    .save()








}
