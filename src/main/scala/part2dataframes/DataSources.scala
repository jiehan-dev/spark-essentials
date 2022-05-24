package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

/*
* --IMPORTANT--
* In order to write the dataframe, we need winutils.exe
* Downloaded from github and put at "C:\ZProgFiles\Hadoop\bin\winutils.exe"
* Setup HADOOP_HOME env variable and path
* */

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    //    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema (optional) or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    //  .option("inferSchema", "true")
    .schema(carsSchema) // enforce a schema
    //    .option("mode", "failFast") // dropMalformed, permissive (default)
    //  .load("src/main/resources/data/cars.json") // pass path as argument or define in option
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDF.show()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
    Writing DFs
    - format
    - save mode = overwrite, ignore, errorIfExists
    - path
    - zero or more options
  */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_duplicate.json")
  //    .option("path", "src/main/resources/data/cars_duplicate.json")
  //    .save()

  // JSON flags
  spark.read
    //    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    //    .load("src/main/resources/data/cars.json")
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .load("src/main/resources/data/stocks.csv")
  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    //    .parquet("src/main/resources/data/cars.parquet")
    .save("src/main/resources/data/cars.parquet") // default to parquet so no need to define format

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //   Reading from a remote DB
  //   Setup before running the following code:
  //   1. 'docker-compose up' in the spark-essentials directory
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc") // Java Database Connectivity (JDBC)
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    * */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // TSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  // save to DB
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
