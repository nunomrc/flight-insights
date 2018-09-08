package object insights {

  val NYMarketID = "31703"

  case class FlightRow(airlineID: String,
                       airLineName: String,
                       destCityMarketID: String,
                       destCityMarketName: String,
                       destAirportID: String,
                       destAirportName: String,
                       arrDelayMinutes: Double)

  case class SortKey(destAirportID: String, airlineID: String, arrDelayMinutes: Double)

  object SortKey {
    implicit def orderingByDelay[A <: SortKey] : Ordering[A] = {
      Ordering.by(k => (k.destAirportID, k.airlineID, k.arrDelayMinutes * -1))
    }
  }

  def timed[T](opName: String)(exec: => T): T = {
    val init = System.currentTimeMillis()
    val result = exec
    println(s"Operation $opName took ${System.currentTimeMillis() - init} milliseconds.")
    result
  }
}
