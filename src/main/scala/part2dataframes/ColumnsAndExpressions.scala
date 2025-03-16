package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

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










}


