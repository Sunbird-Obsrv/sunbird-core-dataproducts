package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext


object CompetencyMetricsTest extends Serializable {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("CompetencyMetricsTest")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", "10.0.0.7")
        .config("spark.cassandra.output.batch.size.rows", "10000")
        //.config("spark.cassandra.read.timeoutMS", "60000")
        .getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val fc: FrameworkContext = new FrameworkContext()
    val res = time(test());
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def test()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val timestamp = System.currentTimeMillis()
    val config = Map(
      "sideOutput" -> Map(
        "brokerList" -> "10.0.0.5:9092",
        "topics" -> Map(
          "expectedCompetencyTopic" -> "dev.dashboards.competency.expected",
          "declaredCompetencyTopic" -> "dev.dashboards.competency.declared",
          "courseCompetencyTopic" -> "dev.dashboards.competency.course",
          "competencyGapTopic" -> "dev.dashboards.competency.gap"
        )
      )
    )
    CompetencyMetricsModel.processCompetencyMetricsData(timestamp, config)
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

}
