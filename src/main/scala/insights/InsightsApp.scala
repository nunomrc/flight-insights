package insights

import org.apache.spark.sql.SparkSession

object InsightsApp {
  def main(args: Array[String]) {

    val sourceFolder = args(0)

    println("Starting spark session...")
    val spark: SparkSession = SparkSession.builder
      .appName("Flight Insights Application")
      .getOrCreate()

    import spark.implicits._

    println("Reading data...")
    val joinedMaster = timed("operation-a")(
      FlightInsights.readData(sourceFolder, spark)
    )
    timed("operation-b")(
      FlightInsights.leastDelay(spark, joinedMaster)
        .coalesce(1)
        .write.csv("result/b")
    )
    timed("operation-c")(
      FlightInsights.mostFlightsTo(spark, joinedMaster, NYMarketID)
        .toDF()
        .coalesce(1)
        .write.csv("result/c")
    )

    val dataRDD = FlightInsights.toRDD(joinedMaster).cache()

    timed("operation-d")(
      FlightInsights.worstAirlineAirportCombination(dataRDD)
        .toDF()
        .coalesce(1)
        .write.csv("result/d")
    )
    timed("operation-e")(
      FlightInsights.worstAirlineAirportCombinationPartitioned(dataRDD)
        .toDF()
        .coalesce(1)
        .write.csv("result/e")
    )

    spark.stop()

  }
}
