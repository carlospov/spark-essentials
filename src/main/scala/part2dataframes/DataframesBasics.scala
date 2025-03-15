package part2dataframes
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataframesBasics extends App{

  // we need a spark session everytime
  val spark = SparkSession.builder()
    .appName("Datframes Basics")
    .config("spark.master","local") // to execute on our computer, on production will be an url where spark is deployed
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format("json") // format to read from
    .option("inferSchema","true") // figure out data types from json structure
    .load("src/main/resources/data/cars.json")

  // showing a DataFrame
  firstDF.show()
  firstDF.printSchema()

  // dataframe = schema + rows
  // way of getting rows from a dataframe
  firstDF.take(10).foreach(println) // take 10 first rows and print them

  // spark infers schema using pattern matching at runtime, spark types are case objects

  // spark types are case objects
  val longType = LongType

  // schema, do this in production, don't infer. It can confuse, for instance, it infers dates as strings
  val carSchema = StructType(
    Array(
      StructField("Name", StringType, nullable = true),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // read DF with own Schema
  val carsDFwithSchema = spark.read
    .format("json")
    .load("src/main/resources/data/cars.json")

  //

  carsDFwithSchema.show()

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create df by hand from tuples
  val myCars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(myCars) // schema auto-inferred but no column names

  manualCarsDF.show()
  manualCarsDF.printSchema()
  // SCHEMAS ARE FOR DATAFRAMES, NOT ROWS

  // create DFs with implicits
  import spark.implicits._
  val manualcarsDFwithImplicits = myCars.toDF("Name","MPG","Cylinders","Displacement","HP","Weight","Acceleration","Year","Country")
  manualcarsDFwithImplicits.printSchema()

  /**
   * Exercises
   * 1) Create a Manual Dataframe describing smartphones
   *  - make
   *  - model
   *  - screen dimension
   *  - camera megapixels
   *
   * 2) Print to the console
   * 3) Read another file from the data folder (for example the movies json)
   *  - print the schema and count the number of rows
   */

  // create df by hand from tuples
  val smartPhones = Seq(
    ("Apple","iPhone16e",6.1,48),
    ("Apple","iPhone16",6.1,24),
    ("Apple","iPhone15",6.1,24),
    ("Xiaomi","15T Ultra",6.4,108),
    ("Xiaomi","Mi 5",5.4,24)
  )

  // create df with implicits using column names
  val smartPhonesDF = smartPhones.toDF("Maker","Model","Screen Size","Megapixels")

  smartPhonesDF.show()

  // read json
  // reading a DF
  val moviesDF = spark.read
    .format("json") // format to read from
    .option("inferSchema","true") // figure out data types from json structure
    .load("src/main/resources/data/movies.json")

  val moviesRowCount = moviesDF.count()

  println(s"Movies available: $moviesRowCount")

  moviesDF.printSchema()







}
