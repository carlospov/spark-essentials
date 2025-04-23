package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication extends App{
  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  // just load bc is in default parquet format
  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  taxiDF.printSchema()
  /*
  root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
   */
  //println(taxiDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  taxiZonesDF.printSchema()
  /*
  root
 |-- LocationID: integer (nullable = true)
 |-- Borough: string (nullable = true)
 |-- Zone: string (nullable = true)
 |-- service_zone: string (nullable = true)
   */

  /**
   * Questions
   *
   * 1. Which zones have the most pickups/dropoffs overall?
   * 2. What are the peak hours for taxi?
   * 3. How are the trips distributed by length? Why are people taking the cab?
   * 4. What are the peak hours for long/short trips?
   * 5. What are the top 3 pickup/dropoff zones for long/short trips?
   * 6. How are people paying for the ride, on long/short trips?
   * 7. How is the payment type evolving with time?
   * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
   *
   */

  // Question 1
  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID") // group by location
    .agg(count("*").as("totalTrips")) // aggregating number of trips by location
    .join(taxiZonesDF, col("PULocationID") === col("LocationID")) // joining this resulting df on location to taxi_zones
    .drop("LocationID","service_zone") // dropping duplicated and irrelevant cols
    .orderBy(col("totalTrips").desc_nulls_last) // order by desc

  //pickupsByTaxiZoneDF.show()

  // 1b - How popular is manhattan over the rest of borough zones
  val pickupsByBorough = pickupsByTaxiZoneDF.groupBy(col("Borough")) // group by borough zone
    .agg(sum(col("totalTrips")).as("totalTrips")) // sum all trips on that zone and keep same column name
    .orderBy(col("totalTrips").desc_nulls_last) // order descending

  //pickupsByBorough.show()

  /*
  +-------------+----------+
|      Borough|totalTrips|
+-------------+----------+
|    Manhattan|    304266|
|       Queens|     17712|
|      Unknown|      6644|
|     Brooklyn|      3037|
|        Bronx|       211|
|          EWR|        19|
|Staten Island|         4|
+-------------+----------+
   */

  // Data is extremely skewed towards Manhattan --> Proposal: differentiate prices according to the pickup/dropoff area, and by demand

  // 2
  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime"))) // extract hour from pickup datetime
    .groupBy("hour_of_day") // group by that hour
    .agg(count("*").as("totalTrips")) // aggregate on count of rows, naming "totalTrips"
    .orderBy(col("totalTrips").desc_nulls_last) // order descending by total trips

  //pickupsByHourDF.show()
  /*
+-----------+----------+
|hour_of_day|totalTrips|
+-----------+----------+
|         17|     22121|
|         18|     21598|
|         20|     20884|
|         19|     20318|
|         21|     19528|
|          7|     18867|
|         16|     18664|
|         14|     17843|
|         13|     17483|
|          8|     16840|
|         15|     16160|
|         12|     16082|
|         11|     16001|
|         10|     15564|
|          6|     15445|
|          9|     15348|
|         22|     14652|
|          5|      8600|
|         23|      7050|
|          0|      3978|
+-----------+----------+
   */

  // There are clear peak hours with increased demand (17:00 - 22:00) and 7:00
  // --> Proposal: differentiate prices according to demand

  // 3
  val tripDistanceDF = taxiDF
    .select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30 // 30 miles

  // statistic on the tripDistanceDF
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )
  //tripDistanceStatsDF.show()
  /*
+------+---------+-----------------+-----------------+---+----+
| count|threshold|             mean|           stddev|min| max|
+------+---------+-----------------+-----------------+---+----+
|331893|       30|2.717989442380494|3.485152224885052|0.0|66.0|
+------+---------+-----------------+-----------------+---+----+
   */
  // minimum is 0, mean is very low and stddev is little --> we expect the vast majority to be short trips
  // let's check
  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold) // Boolean column that marks long trips
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count() // how many long trips are there?
  // tripsByLengthDF.show()
  /*
+------+------+
|isLong| count|
+------+------+
|  true|    83|
| false|331810|
+------+------+
   */
  // there's so few data points on long trips that might not be useful to analyze some of the questions for that class
  // we'll do it anyway

  // 4
  val pickupsByHourByLengthDF = tripsWithLengthDF // tripsWithLengthDF = taxiDF + isLong column
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime"))) // extract hour from pickup datetime
    .groupBy("hour_of_day","isLong") // group by that hour
    .agg(count("*").as("totalTrips")) // aggregate on count of rows, naming "totalTrips"
    .orderBy(col("totalTrips").desc_nulls_last) // order descending by total trips
  pickupsByHourByLengthDF.show(48) // 48 rows is the entire df

  /*
+-----------+------+----------+
|hour_of_day|isLong|totalTrips|
+-----------+------+----------+
|         17| false|     22119|
|         18| false|     21589|
|         20| false|     20874|
|         19| false|     20314|
|         21| false|     19525|
|          7| false|     18862|
|         16| false|     18662|
|         14| false|     17840|
|         13| false|     17478|
|          8| false|     16834|
|         15| false|     16155|
|         12| false|     16077|
|         11| false|     15998|
|         10| false|     15561|
|          6| false|     15441|
|          9| false|     15346|
|         22| false|     14647|
|          5| false|      8600|
|         23| false|      7049|
|          0| false|      3975|
|          4| false|      3133|
|          1| false|      2536|
|          2| false|      1609|
|          3| false|      1586|
|         20|  true|        10|
|         18|  true|         9|
|          8|  true|         6|
|          7|  true|         5|
|         15|  true|         5|
|         12|  true|         5|
|         13|  true|         5|
|         22|  true|         5|
|          6|  true|         4|
|         19|  true|         4|
|         11|  true|         3|
|         10|  true|         3|
|          0|  true|         3|
|         21|  true|         3|
|         14|  true|         3|
|         17|  true|         2|
|          9|  true|         2|
|          1|  true|         2|
|         16|  true|         2|
|          2|  true|         1|
|         23|  true|         1|
+-----------+------+----------+
   */

  // Because of the way the dataset is distributed (totally skewed), question 5 is not interesting, we're gonna do it anyway
  // 5
  val pickupDropoffPopularityDF = tripsWithLengthDF
    .where(not(col("isLong"))) // select trips that are short
    .groupBy("PULocationID","DOLocationID") // grouped both by pickup and dropoff location IDs
    .agg(count("*").as("totalTrips")) // count total trips for each pair of pickup/dropoff IDs
/*
+------------+------------+----------+
|PULocationID|DOLocationID|totalTrips|
+------------+------------+----------+
|         148|         229|        33|
|         163|           7|        36|
|         114|         223|         5|
|          25|          61|         5|
|         114|         151|        14|
|         163|         263|       130|
|         107|         161|       344|
|          49|          49|        11|
|         232|          45|        15|
|         100|         140|        96|
|         132|         107|        75|
|          37|         227|         1|
|         231|         140|        39|
|         229|         239|        69|
|          43|           7|         4|
|          65|          50|         4|
|         244|          41|         4|
|         231|          41|        10|
|         259|         259|         1|
|         116|         134|         1|
+------------+------------+----------+
 */
    .join(taxiZonesDF, col("PULocationID") === col("LocationID")) // join with taxi zones (pickup zone on zone ID)
    .withColumnRenamed("Zone", "Pickup_Zone") // rename the resulting Zone inherited from taxiZonesDF to Pickup_Zone, since join is made on pickup zone ID
    .drop("LocationID","Borough","service_zone") // drop duplicated and useless columns from the resulting DF
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID")) // same but for dropoff
    .withColumnRenamed("Zone","Dropoff_Zone") // same but for dropoff
    .drop("LocationID","Borough","service_zone") // drop duplicated and useless columns from the resulting DF
    .drop("PULocationID","DOLocationID") // Are the ID, not the names
    .orderBy(col("totalTrips").desc_nulls_last)

  //pickupDropoffPopularityDF.show()

  // we can do the same thing for long trips using a function
  def pickupDropoffPopularity(predicate: Column) = tripsWithLengthDF
    .where(predicate) // select trips that are true on a boolean column
    .groupBy("PULocationID","DOLocationID") // grouped both by pickup and dropoff location IDs
    .agg(count("*").as("totalTrips")) // count total trips for each pair of pickup/dropoff IDs
    .join(taxiZonesDF, col("PULocationID") === col("LocationID")) // join with taxi zones (pickup zone on zone ID)
    .withColumnRenamed("Zone", "Pickup_Zone") // rename the resulting Zone inherited from taxiZonesDF to Pickup_Zone, since join is made on pickup zone ID
    .drop("LocationID","Borough","service_zone") // drop duplicated and useless columns from the resulting DF
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID")) // same but for dropoff
    .withColumnRenamed("Zone","Dropoff_Zone") // same but for dropoff
    .drop("LocationID","Borough","service_zone") // drop duplicated and useless columns from the resulting DF
    .drop("PULocationID","DOLocationID") // Are the ID, not the names
    .orderBy(col("totalTrips").desc_nulls_last)

  pickupDropoffPopularity(col("isLong")).show()
  pickupDropoffPopularity(not(col("isLong"))).show()

  // There is a clear separation between long/short trips:
  // --> Short trips are between wealthy NY zones
  // --> Long trips are usually between airports
  // Proposal for the NYC town hall: airport rapid transit
  // Proposal for the taxi company:
  //        - separate market segments and tailor services for each
  //        - Strike a partnership with bars/restaurants on wealthy zones for pickup service









}

