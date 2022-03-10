package org.ekstep.analytics.dashboard

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

import scala.Console

import org.apache.spark.sql.functions.{lit, schema_of_json, from_json}
import collection.JavaConverters._

//

object CompetencyTest extends Serializable {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("CompetencyTest")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", "10.0.0.7")
        .config("spark.cassandra.output.batch.size.rows", "1000")
        //.config("spark.cassandra.read.timeoutMS", "60000")
        .getOrCreate()
    val res = time(compData());
    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

  def compData()(implicit spark: SparkSession): Unit = {
    decCompetencyData()
    druidCompetencyData()
  }

  def druidCompetencyData()(implicit spark: SparkSession) = {
    val druidHost = "10.0.0.13"
    val url = s"http://${druidHost}:8888/druid/v2/sql"
    val requestBody = """{"resultFormat":"array","header":true,"context":{"sqlOuterLimit":10000},"query":"SELECT edata_cb_object_org, edata_cb_data_wa_id, edata_cb_data_wa_userId, edata_cb_data_wa_competency_id, edata_cb_data_wa_competency_level, edata_cb_data_wa_competency_name, edata_cb_data_wa_competency_status, edata_cb_data_wa_competency_type FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"}"""

    // create an HttpPost object
    val post = new HttpPost(url)

    // set the Content-type
    post.setHeader("Content-type", "application/json")

    // add the JSON as a StringEntity
    post.setEntity(new StringEntity(requestBody))

    // send the post request
    val response = (new DefaultHttpClient).execute(post)

    val result = EntityUtils.toString(response.getEntity)

    Console.println("result", result)
  }


  def decCompetencyData()(implicit spark: SparkSession) {

    val userdata = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", "sunbird").option("table", "user").load().persist(StorageLevel.MEMORY_ONLY);

    Console.println("Number of users", userdata.count());

    val userprofile = userdata.where(userdata.col("profiledetails").isNotNull).select("userid", "profiledetails")

//    val profileschema = StructType(Seq(
//      StructField("k", StringType, true), StructField("v", DoubleType, true)
//    ))
//
//    val profileschema2 = StructType(Seq(
//      StructField("competencies", StringType, true), StructField("v", DoubleType, true)
//    ))

    userprofile.where(userdata.col("userid").equalTo("75388267-e5f3-449a-84cb-1d15b81c76ed"))

  }

  def courseCompetencyData()(implicit spark: SparkSession): Unit = {
    val coursedata = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", "dev_hierarchy_store").option("table", "content_hierarchy").load().persist(StorageLevel.MEMORY_ONLY);

    val coursedata2 = coursedata.select("identifier", "hierarchy")

  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

}
