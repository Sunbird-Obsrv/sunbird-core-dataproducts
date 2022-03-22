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
import org.apache.spark.sql.functions.{col, explode_outer, expr, from_json, lit, max}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher


case class DummyInput(timestamp: Long) extends AlgoInput
case class DummyOutput() extends Output with AlgoOutput


object CompetencyMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CompetencyMetricsModel"
  override def name() = "CompetencyMetricsModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCompetencyData(timestamp, config)
    sc.parallelize(Seq())
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())
  }

  def processCompetencyData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // get broker and topics from model config
    val broker = getConfigSideBroker(config)
    val expectedCompetencyTopic = getConfigSideTopic(config, "expectedCompetencyTopic")
    val declaredCompetencyTopic = getConfigSideTopic(config, "declaredCompetencyTopic")
    val courseCompetencyTopic = getConfigSideTopic(config, "courseCompetencyTopic")
    val competencyGapTopic = getConfigSideTopic(config, "competencyGapTopic")

    // get expected competency data, dispatch to kafka
    val ecDF = expectedCompetencyDataFrame()
    kafkaDispatch(withTimestamp(ecDF, timestamp), broker, expectedCompetencyTopic)

    // get declared competency data, dispatch to kafka
    val dcDF = declaredCompetencyDataFrame()
    kafkaDispatch(withTimestamp(dcDF, timestamp), broker, declaredCompetencyTopic)

    // get course competency map, dispatch to kafka
    val ccDF = courseCompetencyDataFrame()
    kafkaDispatch(withTimestamp(ccDF, timestamp), broker, courseCompetencyTopic)

    // calculate competency gaps, dispatch to kafka
    val cgDF = competencyGapDataFrame(ecDF, dcDF)
    kafkaDispatch(withTimestamp(cgDF, timestamp), broker, competencyGapTopic)

  }

  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def test()(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
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
    processCompetencyData(System.currentTimeMillis(), config.asInstanceOf[Map[String, AnyRef]])
  }

  def getConfigSideBroker(config: Map[String, AnyRef]): String = {
    val sideOutput = config.getOrElse("sideOutput", Map()).asInstanceOf[Map[String, AnyRef]]
    sideOutput.getOrElse("brokerList", "").asInstanceOf[String]
  }

  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = {
    val sideOutput = config.getOrElse("sideOutput", Map()).asInstanceOf[Map[String, AnyRef]]
    val topics = sideOutput.getOrElse("topics", Map()).asInstanceOf[Map[String, AnyRef]]
    topics.getOrElse(key, "").asInstanceOf[String]
  }

  def kafkaDispatch(data: DataFrame, broker: String, topic: String)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val stringData = data.toJSON.rdd
    val config = Map("brokerList" -> broker, "topic" -> topic)
    KafkaDispatcher.dispatch(config, stringData)
  }

  def competencyGapDataFrame(ecDF: DataFrame, dcDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    var gapDF = ecDF.join(dcDF,
      ecDF.col("competency_id") <=> dcDF.col("id") && ecDF.col("user_id") <=> dcDF.col("uid"),
      "leftouter"
    )
    gapDF = gapDF.na.fill(0, Seq("declared_level"))  // if null values created during join fill with 0
    gapDF = gapDF.groupBy("user_id", "competency_id", "org_id", "work_order_id")
      .agg(
        max("competency_level").alias("expectedLevel"),  // in-case of multiple entries, take max
        max("declared_level").alias("declaredLevel")  // in-case of multiple entries, take max
      )
    gapDF = gapDF.select(
      col("user_id").alias("userID"),
      col("competency_id").alias("competencyID"),
      col("org_id").alias("orgID"),
      col("work_order_id").alias("workOrderID"),
      col("expectedLevel"),
      col("declaredLevel")
    )
    gapDF = gapDF.withColumn("competencyGap", expr("expectedLevel - declaredLevel"))

    gapDF.show()
    gapDF.printSchema()
    gapDF
  }

  def api(method: String, url: String, body: String): String = {
    val request = method.toLowerCase() match {
      case "post" => new HttpPost(url)
      case _ => throw new Exception(s"HTTP method '${method}' not supported")
    }
    request.setHeader("Content-type", "application/json")  // set the Content-type
    request.setEntity(new StringEntity(body))  // add the JSON as a StringEntity
    val httpClient = HttpClientBuilder.create().build()  // create HttpClient
    val response = httpClient.execute(request)  // send the request
    EntityUtils.toString(response.getEntity)
  }

  def druidSQLAPI(query: String, host: String, resultFormat: String = "object", limit: Int = 10000): String = {
    // TODO: tech-debt, use proper spark druid connector
    val url = s"http://${host}:8888/druid/v2/sql"
    val requestBody = s"""{"resultFormat":"${resultFormat}","header":true,"context":{"sqlOuterLimit":${limit}},"query":"${query}"}"""
    api("POST", url, requestBody)
  }

  def expectedCompetencyDataFrame()(implicit spark: SparkSession) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS org_id, edata_cb_data_wa_id AS work_order_id, edata_cb_data_wa_userId AS user_id, edata_cb_data_wa_competency_id AS competency_id, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS competency_level FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    val result = druidSQLAPI(query, "10.0.0.13")

    import spark.implicits._
    val dataset = spark.createDataset(result :: Nil)
    val df = spark.read.option("multiline", value = true).json(dataset)
      .filter(col("competency_id").isNotNull && col("competency_level").notEqual(0))
      .withColumn("competency_level", expr("CAST(competency_level as INTEGER)"))  // Important to cast as integer otherwise a cast will fail later on

    df.show()
    df.printSchema()
    df
  }

  def elasticSearchCourseAPI(host: String, limit: Int = 1000): String = {
    val url = s"http://${host}:9200/compositesearch/_search"
    val requestBody = s"""{"from":0,"size":${limit},"_source":["identifier","primaryCategory","status","channel","competencies"],"query":{"bool":{"must":[{"match":{"status":"Live"}},{"match":{"primaryCategory":"Course"}}]}}}"""
    api("POST", url, requestBody)
  }

  def liveCourseDataFrame()(implicit spark: SparkSession): DataFrame = {
    val result = elasticSearchCourseAPI("10.0.0.7")

    import spark.implicits._
    val dataset = spark.createDataset(result :: Nil)
    var courseDF = spark.read.option("multiline", value = true).json(dataset)
    courseDF = courseDF.select(explode_outer(col("hits.hits")).alias("course"))
    courseDF = courseDF.select(col("course._source.identifier").alias("id")).distinct()

    courseDF.show()
    courseDF.printSchema()
    courseDF
  }

  def courseCompetencyDataFrame()(implicit spark: SparkSession): DataFrame = {
    val competenciesSchema = ArrayType(StructType(Seq(
      StructField("id",  StringType, nullable = true),
      StructField("selectedLevelLevel",  StringType, nullable = true)
    )))
    val hierarchySchema = StructType(Seq(
      StructField("status", StringType, nullable = true),
      StructField("channel", StringType, nullable = true),
      StructField("competencies_v3", StringType, nullable = true)
    ))

    val rawCourseDF = spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", "dev_hierarchy_store").option("table", "content_hierarchy").load().persist(StorageLevel.MEMORY_ONLY)

    val liveCourseIDsDF = liveCourseDataFrame()

    var courseDF = liveCourseIDsDF.join(rawCourseDF,
      liveCourseIDsDF.col("id") <=> rawCourseDF.col("identifier"), "inner")
      .filter(col("hierarchy").isNotNull)

    courseDF = courseDF.withColumn("data", from_json(col("hierarchy"), hierarchySchema))

    courseDF = courseDF.select(
      col("id").alias("courseID"),
      col("data.status").alias("courseStatus"),
      col("data.channel").alias("courseChannel"),
      col("data.competencies_v3").alias("competenciesJson")
    ).withColumn("competencies", from_json(col("competenciesJson"), competenciesSchema))

    courseDF = courseDF.select(
      col("courseID"), col("courseStatus"), col("courseChannel"),
      explode_outer(col("competencies")).alias("competency")
    ).filter(col("competency").isNotNull)

    courseDF = courseDF.withColumn("competencyLevel", expr("TRIM(competency.selectedLevelLevel)"))
    courseDF = courseDF.withColumn("competencyLevel",
      expr("IF(competencyLevel RLIKE '[0-9]+', CAST(REGEXP_EXTRACT(competencyLevel, '[0-9]+', 0) AS INTEGER), 1)"))

    courseDF = courseDF.select(
      col("courseID"), col("courseStatus"), col("courseChannel"),
      col("competency.id").alias("courseCompetencyID"),
      col("competencyLevel").alias("courseCompetencyLevel")
    )

    courseDF.show()
    courseDF.printSchema()
    courseDF
  }

  def declaredCompetencyDataFrame()(implicit spark: SparkSession) : DataFrame = {

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