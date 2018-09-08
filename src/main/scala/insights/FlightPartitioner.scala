package insights

import org.apache.spark.Partitioner

class FlightPartitioner(partitions: Int) extends Partitioner {

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[SortKey]
    k.destAirportID.hashCode % numPartitions
  }
}