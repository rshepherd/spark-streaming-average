import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

object DataPump {
  def apply(sc: StreamingContext, queue: mutable.SynchronizedQueue[RDD[Visit]], sleep: Int) {
    for (i <- 1 to 9) {
      val visits = List(
        Visit(i, 1),
        Visit(i, 1),
        Visit(i, 1),
        Visit(i, 2),
        Visit(i, 2),
        Visit(i, 2),
        Visit(i, 3),
        Visit(i, 3),
        Visit(i, 3)
      )

      queue += sc.sparkContext.makeRDD(visits)
      Thread.sleep(sleep)
    }
  }
}