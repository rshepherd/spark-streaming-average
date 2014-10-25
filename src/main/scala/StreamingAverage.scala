import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

case class Visit(id: Long, millis: Long)

case class VisitState(count: Long = 0L, sum: Long = 0L) {
  val avg = sum / scala.math.max(count, 1)

  def +(count: Long, sum: Long) = VisitState (
    this.count + count,
    this.sum + sum
  )

  override def toString = {
    s"Visit($count, $sum, $avg)"
  }
}

object StreamingAverage {

  final val queue = new mutable.SynchronizedQueue[RDD[Visit]]()

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StreamingAverage")
    val ctx = new StreamingContext(conf, Seconds(10))
    ctx.checkpoint("./output")

    // Create a stream of visits from the queue
    val stream = ctx.queueStream(queue).map { x =>
      (x.id, x.millis)
    }

    // Update function that will compute state after each dstream ingestion
    val update = (millis: Seq[Long], state: Option[VisitState]) => {
      val prev = state.getOrElse(VisitState())
      val current = prev + (millis.size, millis.sum)
      Some(current)
    }

    // Update the averages for each id
    val state = stream.updateStateByKey[VisitState](update)

    // Print a snapshot of the state
    state.map(_.toString()).print()

    // Fire 'er up
    ctx.start()
    DataPump(ctx, queue, 9999)
    ctx.stop()
  }
}
