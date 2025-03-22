package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master","local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Create Columns
  val firstColumn = carsDF.col("Name") // column object

  // selecting that column from the DF (projection)
  val carsNamesDF = carsDF.select(firstColumn)

  carsNamesDF.show()

  // various select methods
  // fancier ways using
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"), // must be imported
    column("Weight_in_lbs"), // must also be imported, all three do the same
    'Year, //Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated String, returns a Column object
    expr("Origin") // EXPRESSION
  ).show()

  // select with plain column names
  carsDF.select("Name","Year").show()

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2 // the result is a COLUMN object, it represents a transformation

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  // we can also use expr method ^^

  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  carsWithSelectExprWeightsDF.show()

  //  -----  DF processing ----

  // adding a columns
  val carsWithKg3 = carsDF.withColumn("Weight_in_kg_3",col("Weight_in_lbs") / 2.2)
  // renaming columns
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs","Weight in pounds")
  // careful with column names, when selecting BE SURE TO ESCAPE spaces or other characters with ` `
  carsWithColumnRenamed.selectExpr("`Weight in pounds`").show()
  // remove a column
  carsWithColumnRenamed.drop("Cylinders","Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF_v2 = carsDF.where(col("Origin") =!= "USA")  // careful, use =!= for !=, as != conflicts with Scala not equal
  // and use === for equal

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(column("Horsepower") > 150)
  val americanPowerfulCarsDF_v2 = carsDF.filter((col("Origin") === "USA").and(column("Horsepower") > 150))
  // and is infix
  val americanPowerfulCarsDF_v3 = carsDF.filter(col("Origin") === "USA" and column("Horsepower") > 150)
  val americanPowerfulCarsDF_v4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works if both DF have same Schema

  // distinct values
  val allCountriesDF = allCarsDF.select("Origin").distinct()
  allCountriesDF.show()

  /**
   * Exercises
   *
   * 1. Read the movies dataframe and select 2 columns of your choice
   * 2. Create a new dataframe summing up the total profit of the movie = US_GRoss + Worldwide_Gross + DVD_Sales
   * 3. Select all the comedy movies (on Major_Genre) with IMDB rating above 6
   *
   * For every exercise, use as many versions as possible
   */

  //1
  /*
    val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master","local")
    .getOrCreate()
   */

//  val moviesDF = spark.read
//    .format("json")
//    .option("inferSchema","true")
//    .load("src/main/resources/data/movies.json")

//  val moviesDF = spark.read
//    .option("inferSchema","true")
//    .json("src/main/resources/data/movies.json")


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
    .schema(moviesSchema)
    .option("nullValue","0")
    .json("src/main/resources/data/movies.json")

  val firstMovieColumn = moviesDF.col("Title")
  val secondMovieColumn = moviesDF.col("Release_Date")

  moviesDF.select(
    firstMovieColumn,
    secondMovieColumn
  ).show()

  import spark.implicits._
  moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date")
  ).show()

  moviesDF.select(
    column("Title"),
    'Release_Date
  ).show()
  moviesDF.select(
    $"Title",
    expr("Release_Date") // EXPRESSION
  ).show()

  // 2
//
//  val TotalProfitOnMovies = moviesDF.selectExpr(
//    "Title",
//    "Release_Date",
//    "US_Gross",
//    "Worldwide_Gross",
//    "US_DVD_Sales",
//    "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross"
//  )
//
//  val TotalProfitOnMovies = moviesDF.select(
//    col("Title"),
//    col("Release_Date"),
//    col("US_Gross"),
//    col("Worldwide_Gross"),
//    col("US_DVD_Sales"),
//    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
//  )

  val TotalProfitOnMovies = moviesDF.select("Title","US_Gross","Worldwide_Gross").withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))
  TotalProfitOnMovies.show()


  // 3
  //val goodComedyMovies = moviesDF.filter(col("Major_Genre") === "Comedy").filter(col("IMDB_Rating") >= "6")
//  val goodComedyMovies = moviesDF.where((col("Major_Genre") === "Comedy").and(col("IMDB_Rating") >= "6"))
//  val goodComedyMovies = moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") >= 6)
//  val goodComedyMovies = moviesDF.where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") >= 6)
//  val goodComedyMovies = moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating >= 6")
//  val goodComedyMovies = moviesDF.select("Title","IMDB_Rating").filter("Major_Genre = 'Comedy' and IMDB_Rating >= 6")
  val goodComedyMovies = moviesDF.select("Title","IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") >= 6)

  goodComedyMovies.show()














}


