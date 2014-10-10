spark-streaming-average
============================

An example of streaming average calculation with spark streaming. Build and deploy with below.

sbt package && $SPARK_HOME/bin/spark-submit  --class "VisitStreamAverager"  --master local[2] target/scala-2.10/engagement-metrics_2.10-1.0.jar
