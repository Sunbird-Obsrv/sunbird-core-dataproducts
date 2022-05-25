package org.ekstep.analytics.dashboard

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext


object UserOrgRedisUpdateTest extends Serializable {

  def main(args: Array[String]): Unit = {
    val cassandraHost = testModelConfig().getOrElse("sparkCassandraConnectionHost", "localhost").asInstanceOf[String]
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("UserOrgRedisUpdateTest")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", cassandraHost)
        .config("spark.cassandra.output.batch.size.rows", "10000")
        //.config("spark.cassandra.read.timeoutMS", "60000")
        .getOrCreate()
    implicit val sc: SparkContext = spark.sparkContext
    implicit val fc: FrameworkContext = new FrameworkContext()
    sc.setLogLevel("WARN")
    val res = time(test());
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def testModelConfig(): Map[String, AnyRef] = {
    val modelParams = Map(
      "debug" -> "true",
      "sparkCassandraConnectionHost" -> "10.0.0.7",
      "cassandraUserKeyspace" -> "sunbird",
      "cassandraUserTable" -> "user",
      "cassandraOrgTable" -> "organisation",
      "redisRegisteredOfficerCountKey" -> "mdo_registered_officer_count",
      "redisTotalOfficerCountKey" -> "mdo_total_officer_count",
      "redisOrgNameKey" -> "mdo_name_by_org",
      "redisHost" -> "10.0.0.6",
      "redisPort" -> "6379",
      "redisDB" -> "12"
    )
    modelParams
  }

  def test()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    val timestamp = System.currentTimeMillis()
    val config = testModelConfig()
    UserOrgRedisUpdateModel.updateRedisUserOrgData(timestamp, config)
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

}
