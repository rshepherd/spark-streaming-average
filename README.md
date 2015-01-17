spark-streaming-average
============================

An example of streaming average calculation with spark streaming. Build and deploy with below (or something like it).

sbt package && $SPARK_HOME/bin/spark-submit  --class "StreamingAverage"  --master local[2] target/scala-2.10/spark-streaming-average_2.10-1.0.jar
