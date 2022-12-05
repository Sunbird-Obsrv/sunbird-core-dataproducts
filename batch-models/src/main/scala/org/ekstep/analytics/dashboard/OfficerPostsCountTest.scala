package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext

object OfficerPostsCountTest extends Serializable {
  def main(args: Array[String]): Unit = {
    implicit val mongoUri = testModelConfig().getOrElse("sparkMongoConnectionHost", "localhost").asInstanceOf[String]
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("OfficerPostsCountTest")
        .config("spark.master", "local[*]")
        .config("spark.mongodb.input.uri", mongoUri)
        .config("spark.mongodb.input.sampleSize", "50000")
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true")
        .getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val fc: FrameworkContext = new FrameworkContext()
    sc.setLogLevel("WARN")
    val res = time(test());
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val sideOutput = Map(
      "brokerList" -> "10.0.0.5:9092",
      "compression" -> "snappy",
      "topics" -> Map(
        "postCount" -> "dev.dashboards.posts"
      )
    )
    val modelParams = Map(
      "debug" -> "true",
      "sparkCassandraConnectionHost" -> "10.0.0.7",
      "sparkMongoConnectionHost" -> "10.0.0.7",
      "sparkDruidRouterHost" -> "10.0.0.13",
      "sparkElasticsearchConnectionHost" -> "10.0.0.7",
      "cassandraUserKeyspace" -> "sunbird",
      "cassandraUserTable" -> "user",
      "redisHost" -> "10.0.0.6",
      "redisPort" -> "6379",
      "redisDB" -> "12",
      "sideOutput" -> sideOutput
    )
    modelParams
  }

  def test()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val timestamp = System.currentTimeMillis()
    val config = testModelConfig()
    OfficerPostsCountModel.processOfficerDashboardData(timestamp, config)
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }
}
