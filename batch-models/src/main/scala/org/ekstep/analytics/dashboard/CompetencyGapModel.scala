package org.ekstep.analytics.dashboard

import java.io.Serializable
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode_outer, expr, from_json, lit}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._

import org.ekstep.analytics.framework.util.JSONUtils


case class DummyInput(timestamp: Long) extends AlgoInput
@scala.beans.BeanInfo
case class CompetencyGapDataRow(userID: String, competencyID: String, orgID: String, workOrderID: String,
                                expectedLevel: Int, declaredLevel: Int, competencyGap: Int, timestamp: Long) extends Output with AlgoOutput


object CompetencyGapModel extends IBatchModelTemplate[String, DummyInput, CompetencyGapDataRow, CompetencyGapDataRow] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CompetencyGapModel"
  override def name(): String = "CompetencyGapModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CompetencyGapDataRow] = {
    val executionTime = data.first().timestamp
    competencyGapRDD(executionTime)
  }

  override def postProcess(data: RDD[CompetencyGapDataRow], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CompetencyGapDataRow] = {
    data
  }

  def competencyGapRDD(timestamp : Long)(implicit sc: SparkContext, fc: FrameworkContext) : RDD[CompetencyGapDataRow] = {
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    val df = competencyGapData(timestamp)

    df.rdd.map(
      row => CompetencyGapDataRow(
        row.getAs[String]("userID"),
        row.getAs[String]("competencyID"),
        row.getAs[String]("orgID"),
        row.getAs[String]("workOrderID"),
        row.getAs[Int]("expectedLevel"),
        row.getAs[Int]("declaredLevel"),
        row.getAs[Int]("competencyGap"),
        row.getAs[Long]("timestamp")
      )
    )

  }

  def competencyGapData(timestamp : Long)(implicit spark: SparkSession): DataFrame = {
    val ecDF = expectedCompetencyData()
    val dcDF = declaredCompetencyData()

    var gapDF = ecDF.join(dcDF,
      ecDF.col("competency_id") <=> dcDF.col("id") && ecDF.col("user_id") <=> dcDF.col("uid"),
      "leftouter"
    )
    gapDF = gapDF.na.fill(0, Seq("declared_level"))  // if null values created during join fill with 0
    gapDF = gapDF.select(
      col("user_id").alias("userID"),
      col("competency_id").alias("competencyID"),
      col("org_id").alias("orgID"),
      col("work_order_id").alias("workOrderID"),
      col("competency_level").alias("expectedLevel"),
      col("declared_level").alias("declaredLevel")
    )
    gapDF = gapDF.withColumn("competencyGap", expr("expectedLevel - declaredLevel"))
    gapDF = gapDF.withColumn("timestamp", lit(timestamp))

    gapDF.show()
    gapDF.printSchema()
    gapDF
  }

  def druidSQLAPI(query: String, host : String, resultFormat : String = "object", limit : Int = 10000) : String = {
    // TODO: tech-debt, use proper spark druid connector
    val url = s"http://${host}:8888/druid/v2/sql"
    val requestBody = s"""{"resultFormat":"${resultFormat}","header":true,"context":{"sqlOuterLimit":${limit}},"query":"${query}"}"""
    val post = new HttpPost(url)  // create an HttpPost object
    post.setHeader("Content-type", "application/json")  // set the Content-type
    post.setEntity(new StringEntity(requestBody))  // add the JSON as a StringEntity
    val httpClient = HttpClientBuilder.create().build()  // create HttpClient
    val response = httpClient.execute(post)  // send the post request
    EntityUtils.toString(response.getEntity)
  }

  def expectedCompetencyData()(implicit spark: SparkSession) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS org_id, edata_cb_data_wa_id AS work_order_id, edata_cb_data_wa_userId AS user_id, edata_cb_data_wa_competency_id AS competency_id, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS competency_level FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    val result = druidSQLAPI(query, "10.0.0.13")

    import spark.implicits._
    val dataset = spark.createDataset(result :: Nil)
    val df = spark.read.option("multiline", value = true).json(dataset)
      .filter(col("competency_id").isNotNull && col("competency_level").notEqual(0))

    df.show()
    df.printSchema()
    df
  }

  def declaredCompetencyData()(implicit spark: SparkSession) : DataFrame = {

    val competencySchema = StructType(Seq(
      StructField("id",  StringType, nullable = true),
      StructField("name",  StringType, nullable = true),
      StructField("status",  StringType, nullable = true),
      StructField("competencyType",  StringType, nullable = true),
      StructField("competencySelfAttestedLevel",  IntegerType, nullable = true)
    ))
    val profileSchema = StructType(
      StructField("competencies", ArrayType(competencySchema), nullable = true) :: Nil
    )

    val userdata = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", "sunbird").option("table", "user").load().persist(StorageLevel.MEMORY_ONLY);

    Console.println("Number of users", userdata.count());

    val up = userdata.where(col("profiledetails").isNotNull)
      .select("userid", "profiledetails")
      .withColumn("profile", from_json(col("profiledetails"), profileSchema))
      .select(col("userid"), explode_outer(col("profile.competencies")).alias("competency"))
      .where(col("competency").isNotNull && col("competency.id").isNotNull)
      .select(
        col("userid").alias("uid"),
        col("competency.id").alias("id"),
        col("competency.competencySelfAttestedLevel").alias("declared_level")
      )
      .na.fill(1, Seq("declared_level"))  // if competency is listed without a level assume level 1

    up.show()
    up.printSchema()
    up
  }

}