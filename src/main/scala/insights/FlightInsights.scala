package insights

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}

import scala.util.Try

object FlightInsights {

  private def masterFile(folder: String) = s"$folder/On_Time_On_Time_Performance_2018_1.csv"
  private def lookupAirlineFile(folder: String) = s"$folder/L_AIRLINE_ID.csv"
  private def lookupCityMarketFile(folder: String) = s"$folder/L_CITY_MARKET_ID.csv"
  private def lookupAirportFile(folder: String) = s"$folder/L_AIRPORT_ID.csv"

  private val numPartitions = 6

  def readData(folder: String, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val master = spark
      .read
      .option("header", "true")
      .csv(masterFile(folder))
      .select($"AirlineID", $"DestCityMarketID", $"DestAirportID", $"ArrDelayMinutes".as[Double])
      .na.fill("0.0", Seq("ArrDelayMinutes"))

    val airlines = broadcast(
      spark.read.option("header", "true").csv(lookupAirlineFile(folder))
        .select(
          col("Code").as("AirlineCode"),
          col("Description").as("AirlineName")
        )
    )

    val cityMarkets = broadcast(
      spark.read.option("header", "true").csv(lookupCityMarketFile(folder))
        .select(
          col("Code").as("CityMarketCode"),
          col("Description").as("CityMarketName")
        )
    )

    val airports = broadcast(
      spark.read.option("header", "true").csv(lookupAirportFile(folder))
        .select(
          col("Code").as("AirportCode"),
          col("Description").as("AirportName")
        )
    )

    master
      .join(airlines, master("AirlineID") === airlines("AirlineCode"))
      .join(cityMarkets, master("DestCityMarketID") === cityMarkets("CityMarketCode"))
      .join(airports, master("DestAirportID") === airports("AirportCode"))
  }

  private def safeDouble(v: String): Double = Try(v.toDouble).toOption.getOrElse(0d)

  def toRDD(master: DataFrame): RDD[FlightRow] = {
    import master.sparkSession.implicits._
    val ds = master.map { row =>
      FlightRow(
        row.getAs[String]("AirlineID"),
        row.getAs[String]("AirlineName"),
        row.getAs[String]("DestCityMarketID"),
        row.getAs[String]("CityMarketName"),
        row.getAs[String]("DestAirportID"),
        row.getAs[String]("AirportName"),
        safeDouble(row.getAs[String]("ArrDelayMinutes"))
      )
    }

    ds.rdd
  }

  def leastDelay(spark: SparkSession, master: DataFrame): DataFrame = {
    import spark.implicits._

    master
      .select($"AirlineID", $"AirlineName", $"ArrDelayMinutes".as[Double])
      .sort($"ArrDelayMinutes")
  }

  def mostFlightsTo(spark: SparkSession, master: DataFrame, cityMarketID: String): DataFrame = {
    import spark.implicits._

    master
      .select($"AirlineID", $"DestCityMarketID", $"AirlineName")
      .groupBy("AirlineID", "DestCityMarketID", "AirlineName")
      .count()
      .sort($"count".desc)
      .where($"DestCityMarketID" === cityMarketID)
  }

  def worstAirlineAirportCombination(master: RDD[FlightRow]): RDD[FlightRow] = {
    master
      .map(f => SortKey(f.destAirportID, f.airlineID, f.arrDelayMinutes) -> f)
      .sortByKey()
      .values
  }

  def worstAirlineAirportCombinationPartitioned(master: RDD[FlightRow]): RDD[FlightRow] = {
    master
      .map(f => SortKey(f.destAirportID, f.airlineID, f.arrDelayMinutes) -> f)
      .repartitionAndSortWithinPartitions(new FlightPartitioner(numPartitions))
      .values
  }

}
