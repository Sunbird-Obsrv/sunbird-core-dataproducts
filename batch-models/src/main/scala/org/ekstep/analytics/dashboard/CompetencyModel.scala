package org.ekstep.analytics.dashboard

import com.datastax.spark.connector.toSparkContextFunctions

import java.io.Serializable
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.apache.commons.lang3.StringUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework._

import scala.io.Source.fromURL
import org.json4s.jackson.JsonMethods.parse

@scala.beans.BeanInfo
class WFSInputEvent(val eid: String, val ets: Long, val `@timestamp`: String, val ver: String, val mid: String, val actor: Actor, val context: V3Context, val `object`: Option[V3Object], val edata: WFSInputEData, val tags: List[AnyRef] = null) extends AlgoInput with Input {}
@scala.beans.BeanInfo
class WFSInputEData(val `type`: String, val mode: String, val duration: Long, val pageid: String, val item: Question,
                    val resvalues: Array[Map[String, AnyRef]], val pass: String, val score: Int) extends Serializable {}

case class WorkflowInput(sessionKey: WorkflowIndex, events: Buffer[String]) extends AlgoInput
case class WorkflowOutput(index: WorkflowIndex, summaries: Buffer[MeasuredEvent]) extends AlgoOutput
case class WorkflowIndex(did: String, channel: String, pdataId: String)
case class WorkFlowIndexEvent(eid: String, context: V3Context)

case class WorkOrderOfficerCompetency(userID: String, competencyID: String, level: Int)
case class WorkOrderOfficerCompetencyResult(success: Boolean, message: String, result: List[WorkOrderOfficerCompetency])

object CompetencyGapModel extends IBatchModelTemplate[String, WorkflowInput, MeasuredEvent, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.CompetencyModel"
    override def name: String = "CompetencyModel"

    override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkflowInput] = ???

    def druidCompetencyData()(implicit spark: SparkSession) = {
      val druidHost = "localhost"
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

      result
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

  def fracCompetencyData()(implicit spark: SparkSession): Unit = {
    val coursedata = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", "dev_hierarchy_store").option("table", "content_hierarchy").load().persist(StorageLevel.MEMORY_ONLY);

    val coursedata2 = coursedata.select("identifier", "hierarchy")

  }

  override def algorithm(data: RDD[WorkflowInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[MeasuredEvent] = {

    val expectedCompData = druidCompetencyData()



  }

  override def postProcess(data: RDD[MeasuredEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[MeasuredEvent] = {
      data
  }
}