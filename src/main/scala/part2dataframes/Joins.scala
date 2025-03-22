package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, first, max, struct}
import part2dataframes.DataSources.{employeesDF, moviesDF}

object Joins extends App{
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show()

  // outer joins
  // left outer: everything in the inner join + all the rows in the LEFT table with nulls where data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  // right outer: everything in the inner join + all the rows in the RIGHT table with nulls where data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  // full outer: everything in the inner join + all rows in BOTH tables including nulls
  guitaristsDF.join(bandsDF, joinCondition, "outer").show()

  // semi-joins: rows FROM THE LEFT DataFrame that satisfy join condition, but result only stores info from the left dataframe
  // everything in the first df for which there is a row in the right df satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti-join: is a semi-join for ¬joinCondition
  // everything in the first df for which there is NO row in the right df satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // things to bear in mind
  //guitaristsBandsDF.select("id").show() // this crashes, ambiguity in "id" columns
  // there are some options

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop dupe column
  guitaristsBandsDF.drop(bandsDF.col("id")) // id of the original dataframe must be referenced
  // is weird, but spark maintains identifiers for the columns that are used

  //option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))


  // using complex types: you can use any kind of expression as join condition
  guitaristsDF.join(guitarsDF.withColumnRenamed("id","guitarId"), expr("array_contains(guitars, guitarId)")).show()

  /**
   * Exercises
   *  - show all employees and their max salary
   *  - show all employees who where never managers
   *  - find the job titles of the best paid 10 employees in the company
   *
   *
   */

  val salaries = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.salaries")
    .load()

  val employees = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  val titles = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.titles")
    .load()

  val dept_manager = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.dept_manager")
    .load()

  // 1

  val employeesMaxSalary = salaries
    .groupBy(col("emp_no"))
    .agg(max("salary").as("Max_Salary"))
    .orderBy(col("Max_Salary").desc_nulls_last)

  employeesMaxSalary.show()

  employees.join(employeesMaxSalary, employees.col("emp_no") === employeesMaxSalary.col("emp_no"), "left_outer").drop(employees.col("emp_no")).show()

  // 2

  employees.join(dept_manager, employees.col("emp_no") === dept_manager.col("emp_no"), "left_anti").show()

  // 3
  val mostRecentJobTitles = titles.groupBy("emp_no","title").agg(max("to_date"))
  val bestPaidEmployees = employees.join(employeesMaxSalary, "emp_no").orderBy(col("Max_Salary").desc).limit(10)
  val bestPaidJobs = bestPaidEmployees.join(mostRecentJobTitles, "emp_no")

  bestPaidJobs.show()

  // imho this query it's not okay, it asks for the best paid employees TODAY not all-time.
  // Someone whose salary has descended to the 11th but had the top 1 salary once would also appear in the list

}
