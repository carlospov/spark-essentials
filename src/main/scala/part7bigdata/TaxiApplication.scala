package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders}

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
  // pickupsByHourByLengthDF.show(48) // 48 rows is the entire df

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

  //pickupDropoffPopularity(col("isLong")).show()
  //pickupDropoffPopularity(not(col("isLong"))).show()

  // There is a clear separation between long/short trips:
  // --> Short trips are between wealthy NY zones
  // --> Long trips are usually between airports
  // Proposal for the NYC town hall: airport rapid transit
  // Proposal for the taxi company:
  //        - separate market segments and tailor services for each
  //        - Strike a partnership with bars/restaurants on wealthy zones for pickup service

  // 6
  val paymentTypeDistributionDF = taxiDF // Which payment type are most popular?
    .groupBy(col("payment_type")) // for every payment type
    .agg(count("*").as("totalTrips")) // count rows and enconde as totalTrips
    .orderBy(col("totalTrips").desc_nulls_last) // order descending

  //paymentTypeDistributionDF.show()

  /*
+------------+----------+
|payment_type|totalTrips|
+------------+----------+
|           1|    324387|  // 1  = credit card
|           2|      5878|  // 2  = cash
|           5|       895|  // 5  = unknown
|           3|       530|  // 3  = no charge
|           4|       193|  // 4  = dispute
|          99|         7|  // 99 = ???
|           6|         3|  // 6  = voided
+------------+----------+
   */

  // most trips are paid using credit card
  // Cash is dying

  // Question 7 will draw meaningful conclusions when asked the full dataset, the following is the how-to:
  // 7
  val paymentTypeEvolution = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("payment_type"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))

  // paymentTypeEvolution.show()
/*
+----------+------------+----------+
|pickup_day|payment_type|totalTrips|
+----------+------------+----------+
|2018-01-24|           1|      4957|
|2018-01-24|           2|      2026|
|2018-01-24|           3|        52|
|2018-01-24|           4|        15|
|2018-01-25|           3|      1487|
|2018-01-25|           2|     86339|
|2018-01-25|           1|    236640|
|2018-01-25|           4|       377|
+----------+------------+----------+
 */

  // 8
  // first we have to "bucketize" time
  val groupAttempsDF = taxiDF
    .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"), col("PULocationID"), col("total_amount"))
    // unix is the absolute number of seconds since jan 1st 1970
    // dividing by 300 we obtain a 5-minute bucket partition of the time. This expression is double, so round to the nearest integer, and casting the result to integer
    // column named as "fiveMinId", also selecting PULocationID and total_amount
    .where(col("passenger_count") < 3) // cars avg 4 seats
    .groupBy(col("fiveMinId"), col("PULocationID")) // same five minutes and same pickup location
    .agg(count("*").as("total_trips"), sum(col("total_amount").as("total_amount"))) // how many of this groupable trips are there?
    .orderBy(col("total_trips").desc_nulls_last) // rank them
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300)) // reconvert to readable timestamp
    .drop("fiveMinId") // drop unreadable column of the 5-minute buckets
    .join(taxiZonesDF, col("PULocationID") === col("LocationID")) // join with taxi zones dataframe to actually see the name of locations names
    .drop("LocationID", "service_zone") // drop duped ID and names

  // groupAttempsDF.show()

/*
+------------+-----------+---------------------------------+--------------------+---------+--------------------+
|PULocationID|total_trips|sum(total_amount AS total_amount)|approximate_datetime|  Borough|                Zone|
+------------+-----------+---------------------------------+--------------------+---------+--------------------+
|         264|          7|                            119.3| 2018-01-24 23:45:00|  Unknown|                  NV|
|         239|          4|                            45.31| 2018-01-25 00:00:00|Manhattan|Upper West Side S...|
|         140|          5|                            48.96| 2018-01-24 23:15:00|Manhattan|     Lenox Hill East|
|          50|          6|                            96.45| 2018-01-24 23:55:00|Manhattan|        Clinton West|
|         138|          2|                            68.06| 2018-01-24 23:35:00|   Queens|   LaGuardia Airport|
|          61|          1|                            16.55| 2018-01-24 23:05:00| Brooklyn| Crown Heights North|
|         209|          2|                            39.18| 2018-01-25 01:25:00|Manhattan|             Seaport|
|          90|          2|                             44.6| 2018-01-25 01:20:00|Manhattan|            Flatiron|
|         142|          2|               15.600000000000001| 2018-01-25 01:20:00|Manhattan| Lincoln Square East|
|         170|          4|                76.46000000000001| 2018-01-25 02:25:00|Manhattan|         Murray Hill|
|           4|          2|                            41.35| 2018-01-25 02:05:00|Manhattan|       Alphabet City|
|          43|          1|                             18.3| 2018-01-25 03:35:00|Manhattan|        Central Park|
|         226|          1|                             13.3| 2018-01-25 03:15:00|   Queens|           Sunnyside|
|         164|          5|                            87.91| 2018-01-25 03:55:00|Manhattan|       Midtown South|
|         162|          1|                            17.25| 2018-01-25 03:10:00|Manhattan|        Midtown East|
|          50|          1|                              5.3| 2018-01-25 03:25:00|Manhattan|        Clinton West|
|         263|          5|               127.57000000000001| 2018-01-25 04:40:00|Manhattan|      Yorkville West|
|         249|          3|                           136.37| 2018-01-25 04:20:00|Manhattan|        West Village|
|         143|          8|                            94.74| 2018-01-25 05:35:00|Manhattan| Lincoln Square West|
|         107|         26|               307.71000000000004| 2018-01-25 06:15:00|Manhattan|            Gramercy|
+------------+-----------+---------------------------------+--------------------+---------+--------------------+
 */

  // Lots of close taxi rides
  // --> Proposal: Incentivize people to take a grouped ride, at a discount
  //            - lower cost
  //            - more competitive with lower prices
  //            - fewer emissions, so can ask for a subsidy on the project

  // We will make a model for estimating potential economic impact over the dataset
  // Parameters:
  //    - Let's assume we invented a technology that could group taxi rides by sending notifications to users' smartphones and
  //    that can detect 5% of rides as groupable
  //    - Let's assume 30% actually accept to be grouped
  //    - 5$ discount if you take a group ride
  //    - 2$ extra if you take an individual ride (bc of privacy/time)
  //    - Let's assume that if two rides grouped, cost reductions are on the 60% of one avg ride

  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2

  implicit val doubleEncoder = Encoders.scalaDouble
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimatedEconomicImpactDF = groupAttempsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupRidesEconomicImpact") + col("rejectedGroupRidesEconomicImpact"))
    .orderBy(col("totalImpact").desc_nulls_last)

  groupingEstimatedEconomicImpactDF.show(100)

  val totalProfitDF = groupingEstimatedEconomicImpactDF
    .select(sum(col("totalImpact")).as("total"))

  totalProfitDF.show()
  /*
+-----------------+
|            total|
+-----------------+
|39987.73868641883|
+-----------------+
   */
  // 40k dollars in 2 days == 6 millions a year

  // followed lesson of the deployment on EMR but didn't take notes







}

