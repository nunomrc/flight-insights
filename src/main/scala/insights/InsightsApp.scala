package insights

import org.apache.spark.sql.SparkSession

/**
  * App to provide to a spark-submit command, to run as a spark application.
  * Details on how to run are given in the README file.
  *
  * Each transform operation is timed where I try to make it comparable:
  * - operation a) reads the data, joins, and caches it.
  * - the other operations get a cached DataFrame/RDD, make the necessary transformations,
  *   and save the data into a single CSV file. This is done for demonstration purposes only,
  *   in a real case scenario with a big data set running on a cluster, the final result would be
  *   coalesced to a number that would make sense when connecting into the next stage of the data
  *   processing pipeline.
  */
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
      FlightInsights.readData(sourceFolder, spark).cache()
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
