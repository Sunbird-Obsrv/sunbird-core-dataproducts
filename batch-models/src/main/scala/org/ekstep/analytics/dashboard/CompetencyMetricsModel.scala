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
import org.apache.spark.sql.functions.{col, explode_outer, expr, first, from_json, lit, max}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher

/*

Prerequisites(PR) -

PR01: user's expected competencies, declared competencies, and competency gaps
PR02: course competency mapping
PR03: user's course progress
PR04: course rating summary
PR05: all competencies from FRAC


Metric  PR      Type                Description

M2.08   1,2     Scorecard           Number of competencies mapped to MDO officials for which there is no CBP on iGOT
M2.11   1       Scorecard           Average number of competency gaps per officer in the MDO
M2.22   1       Scorecard           Average for MDOs: Average number of competency gaps per officer
M3.55   1       Bar-Graph           Total competency gaps in the MDO
M3.56   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs have not been started by officers
M3.57   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are in progress by officers
M3.58   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are completed by officers

S3.13   1       Scorecard           Average competency gaps per user
S3.11   4       Leaderboard         Average user rating of CBPs
S3.14   1       Bar-Graph           Total competency gaps
S3.15   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs have not been started by officers
S3.16   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are in progress by officers
S3.17   1,2,3   Stacked-Bar-Graph   Percentage of competency gaps for which CBPs are completed by officers

C1.01   5       Scorecard           Total number of CBPs on iGOT platform
C1.1    4       Scorecard           Use ratings averaged for ALL CBPs by the provider
C1.03   3       Scorecard           Number of officers who enrolled (defined as 10% completion) for the CBP in the last year
C1.04   2,3     Bar-Graph           CBP enrollment rate (for a particular competency)
C1.05   3       Scorecard           Number of officers who completed the CBP in the last year
C1.06   3       Leaderboard         CBP completion rate
C1.07   4       Leaderboard         average user ratings by enrolled officers for each CBP
C1.09   5       Scorecard           No. of CBPs mapped (by competency)

*/

case class DummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class DummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

/**
 * Model for processing competency metrics
 * TODO: make more configurable, remove hardcoded hosts etc
 */
object CompetencyMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CompetencyMetricsModel"
  override def name() = "CompetencyMetricsModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(DummyInput(executionTime)))
  }

  override def algorithm(data: RDD[DummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCompetencyMetricsData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[DummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DummyOutput] = {
    sc.parallelize(Seq())  // return empty rdd
  }

  /**
   * Master method, does all the work, fetching, processing and dispatching
   *
   * @param timestamp unique timestamp from the start of the processing
   * @param config model config, should be defined at sunbird-data-pipeline:ansible/roles/data-products-deploy/templates/model-config.j2
   * @param spark
   * @param sc
   * @param fc
   */
  def processCompetencyMetricsData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // get broker and topics from model config
    val broker = getConfigSideBroker(config)
    val userCourseCompletionTopic = getConfigSideTopic(config, "userCourseCompletionTopic")
    val fracCompetencyTopic = getConfigSideTopic(config, "fracCompetencyTopic")
    val courseCompetencyTopic = getConfigSideTopic(config, "courseCompetencyTopic")
    val expectedCompetencyTopic = getConfigSideTopic(config, "expectedCompetencyTopic")
    val declaredCompetencyTopic = getConfigSideTopic(config, "declaredCompetencyTopic")
    val competencyGapTopic = getConfigSideTopic(config, "competencyGapTopic")
    val courseRatingSummaryTopic = getConfigSideTopic(config, "courseRatingSummaryTopic")

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-progress
    val uccDF = userCourseCompletionDataFrame()
    kafkaDispatch(withTimestamp(uccDF, timestamp), broker, userCourseCompletionTopic)

    // get frac competency data, dispatch to kafka to be ingested by druid data-source: dashboards-frac-competency
    val fcDF = fracCompetencyDataFrame()
    kafkaDispatch(withTimestamp(fcDF, timestamp), broker, fracCompetencyTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val ccDF = courseCompetencyDataFrame()
    kafkaDispatch(withTimestamp(ccDF, timestamp), broker, courseCompetencyTopic)

    // get user's expected competency data, dispatch to kafka to be ingested by druid data-source: dashboards-expected-user-competency
    val ecDF = expectedCompetencyDataFrame()
    kafkaDispatch(withTimestamp(ecDF, timestamp), broker, expectedCompetencyTopic)

    // get user's declared competency data, dispatch to kafka to be ingested by druid data-source: dashboards-declared-user-competency
    val dcDF = declaredCompetencyDataFrame()
    kafkaDispatch(withTimestamp(dcDF, timestamp), broker, declaredCompetencyTopic)

    // calculate competency gaps, add course completion status, dispatch to kafka to be ingested by druid data-source: dashboards-user-competency-gap
    val cgDF = competencyGapDataFrame(ecDF, dcDF)
    val cgcDF = competencyGapCompletionDataFrame(cgDF, ccDF, uccDF)  // add course completion status
    kafkaDispatch(withTimestamp(cgcDF, timestamp), broker, competencyGapTopic)

    // get total rating and total number of rating for course rating summary, dispatch to kafka to be ingested by druid data-source: dashboards-course-rating-summary
    val crsDF = courseRatingSummaryDataFrame()
    kafkaDispatch(withTimestamp(crsDF, timestamp), broker, courseRatingSummaryTopic)
  }

  /* Util functions */

  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def getConfigSideBroker(config: Map[String, AnyRef]): String = {
    // TODO: tech-debt: wrong config would result in an uncaught error
    val sideOutput = config.getOrElse("sideOutput", Map()).asInstanceOf[Map[String, AnyRef]]
    sideOutput.getOrElse("brokerList", "").asInstanceOf[String]
  }

  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = {
    // TODO: tech-debt: wrong config would result in an uncaught error
    val sideOutput = config.getOrElse("sideOutput", Map()).asInstanceOf[Map[String, AnyRef]]
    val topics = sideOutput.getOrElse("topics", Map()).asInstanceOf[Map[String, AnyRef]]
    topics.getOrElse(key, "").asInstanceOf[String]
  }

  def kafkaDispatch(data: DataFrame, broker: String, topic: String)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val stringData = data.toJSON.rdd
    val config = Map("brokerList" -> broker, "topic" -> topic)
    KafkaDispatcher.dispatch(config, stringData)
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

  def elasticSearchCourseAPI(host: String, limit: Int = 1000): String = {
    val url = s"http://${host}:9200/compositesearch/_search"
    val requestBody = s"""{"from":0,"size":${limit},"_source":["identifier","primaryCategory","status","channel","competencies"],"query":{"bool":{"must":[{"match":{"status":"Live"}},{"match":{"primaryCategory":"Course"}}]}}}"""
    api("POST", url, requestBody)
  }

  def fracCompetencyAPI(host: String): String = {
    val url = s"https://${host}/graphql"
    val requestBody = """{"operationName":"filterCompetencies","variables":{"cod":[""],"competencyType":[""],"competencyArea":[""]},"query":"query filterCompetencies($cod: [String], $competencyType: [String], $competencyArea: [String]) {\n  getAllCompetencies(\n    cod: $cod\n    competencyType: $competencyType\n    competencyArea: $competencyArea\n  ) {\n    name\n    id\n    description\n    status\n    source\n    additionalProperties {\n      competencyType\n      competencyArea\n      __typename\n    }\n    __typename\n  }\n}\n"}"""
    api("POST", url, requestBody)
  }

  def dataFrameFromJSONString(jsonString: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val dataset = spark.createDataset(jsonString :: Nil)
    spark.read.option("multiline", value = true).json(dataset)
  }

  def cassandraTableAsDataFrame(keySpace: String, table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").option("inferSchema", "true")
      .option("keyspace", keySpace).option("table", table).load().persist(StorageLevel.MEMORY_ONLY)
  }

  /**
   * completionPercentage   completionStatus
   * NULL                   not-enrolled
   * 0.0                    enrolled
   * 0.0 < % < 10.0         started
   * 10.0 <= % < 100.0      in-progress
   * 100.0                  completed
   * @param df data frame with completionPercentage column
   * @return df with completionStatus column
   */
  def withCompletionStatusColumn(df: DataFrame): DataFrame = {
    val caseExpression = "CASE WHEN ISNULL(completionPercentage) THEN 'not-enrolled' WHEN completionPercentage == 0.0 THEN 'enrolled' WHEN completionPercentage < 10.0 THEN 'started' WHEN completionPercentage < 100.0 THEN 'in-progress' ELSE 'completed'"
    df.withColumn("completionStatus", expr(caseExpression))
  }

  /* Data processing functions */

  /**
   * data frame of user course completion percentage
   * @return DataFrame(completionUserID, completionCourseID, completionPercentage, completionStatus)
   */
  def userCourseCompletionDataFrame()(implicit spark: SparkSession): DataFrame = {
    var df = cassandraTableAsDataFrame("sunbird_courses", "user_content_consumption")
      .select(
        col("userid").alias("completionUserID"),
        col("courseid").alias("completionCourseID"),
        col("completionpercentage").alias("completionPercentage")
      )
    df = withCompletionStatusColumn(df)

    df.show()
    df.printSchema()
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api
   * @return DataFrame(fracCompetencyID, fracCompetencyName, fracCompetencyStatus)
   */
  def fracCompetencyDataFrame()(implicit spark: SparkSession): DataFrame = {
    val result = fracCompetencyAPI("frac-dictionary-backend.igot-stage.in")
    val df = dataFrameFromJSONString(result)
      .select(explode_outer(col("data.getAllCompetencies")).alias("competency"))
      .select(
        col("competency.id").alias("fracCompetencyID"),
        col("competency.name").alias("fracCompetencyName"),
        col("competency.status").alias("fracCompetencyStatus")
      )

    df.show()
    df.printSchema()
    df
  }

  /**
   * Distinct live course ids from elastic search api
   * need to do this because otherwise we will have to parse all json records in cassandra to filter live ones
   * @return DataFrame(id)
   */
  def liveCourseDataFrame()(implicit spark: SparkSession): DataFrame = {
    val result = elasticSearchCourseAPI("10.0.0.7")

    var courseDF = dataFrameFromJSONString(result)
    courseDF = courseDF.select(explode_outer(col("hits.hits")).alias("course"))
    courseDF = courseDF.select(col("course._source.identifier").alias("id")).distinct()

    courseDF.show()
    courseDF.printSchema()
    courseDF
  }

  /* schema definitions for courseCompetencyDataFrame */
  val courseCompetenciesSchema: ArrayType = ArrayType(StructType(Seq(
    StructField("id",  StringType, nullable = true),
    StructField("selectedLevelLevel",  StringType, nullable = true)
  )))
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("status", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("competencies_v3", StringType, nullable = true)
  ))
  /**
   * course competency mapping data from cassandra dev_hierarchy_store:content_hierarchy
   * @return DataFrame(courseID, courseStatus, courseChannel, courseCompetencyID, courseCompetencyLevel)
   */
  def courseCompetencyDataFrame()(implicit spark: SparkSession): DataFrame = {

    val rawCourseDF = cassandraTableAsDataFrame("dev_hierarchy_store", "content_hierarchy")
    val liveCourseIDsDF = liveCourseDataFrame()  // get ids for live courses from es api

    // inner join so that we only retain live courses
    var courseDF = liveCourseIDsDF.join(rawCourseDF,
      liveCourseIDsDF.col("id") <=> rawCourseDF.col("identifier"), "inner")
      .filter(col("hierarchy").isNotNull)

    courseDF = courseDF.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
    courseDF = courseDF.select(
      col("id").alias("courseID"),
      col("data.status").alias("courseStatus"),
      col("data.channel").alias("courseChannel"),
      col("data.competencies_v3").alias("competenciesJson")
    ).withColumn("competencies", from_json(col("competenciesJson"), courseCompetenciesSchema))

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

  /**
   * User's expected competency data from the latest approved work orders issued for them from druid
   * @return DataFrame(expOrgID, expWorkOrderID, expUserID, expCompetencyID, expCompetencyLevel)
   */
  def expectedCompetencyDataFrame()(implicit spark: SparkSession) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS expOrgID, edata_cb_data_wa_id AS expWorkOrderID, edata_cb_data_wa_userId AS expUserID, edata_cb_data_wa_competency_id AS expCompetencyID, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS expCompetencyLevel FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    val result = druidSQLAPI(query, "10.0.0.13")

    val df = dataFrameFromJSONString(result)
      .filter(col("expCompetencyID").isNotNull && col("expCompetencyLevel").notEqual(0))
      .withColumn("expCompetencyLevel", expr("CAST(expCompetencyLevel as INTEGER)"))  // Important to cast as integer otherwise a cast will fail later on

    df.show()
    df.printSchema()
    df
  }

  /* schema definitions for declaredCompetencyDataFrame */
  val profileCompetencySchema: StructType = StructType(Seq(
    StructField("id",  StringType, nullable = true),
    StructField("name",  StringType, nullable = true),
    StructField("status",  StringType, nullable = true),
    StructField("competencyType",  StringType, nullable = true),
    StructField("competencySelfAttestedLevel",  IntegerType, nullable = true)
  ))
  val profileDetailsSchema: StructType = StructType(
    StructField("competencies", ArrayType(profileCompetencySchema), nullable = true) :: Nil
  )
  /**
   * User's declared competency data from cassandra sunbird:user
   * @return DataFrame(decUserID, decCompetencyID, decCompetencyLevel)
   */
  def declaredCompetencyDataFrame()(implicit spark: SparkSession) : DataFrame = {
    val userdata = cassandraTableAsDataFrame("sunbird", "user")

    val up = userdata.where(col("profiledetails").isNotNull)
      .select("userid", "profiledetails")
      .withColumn("profile", from_json(col("profiledetails"), profileDetailsSchema))
      .select(col("userid"), explode_outer(col("profile.competencies")).alias("competency"))
      .where(col("competency").isNotNull && col("competency.id").isNotNull)
      .select(
        col("userid").alias("decUserID"),
        col("competency.id").alias("decCompetencyID"),
        col("competency.competencySelfAttestedLevel").alias("decCompetencyLevel")
      )
      .na.fill(1, Seq("decCompetencyLevel"))  // if competency is listed without a level assume level 1

    up.show()
    up.printSchema()
    up
  }

  /**
   * Calculates user's competency gaps
   * @param ecDF expected competency data frame
   * @param dcDF declared  competency data frame
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedLevel, declaredLevel, competencyGap)
   */
  def competencyGapDataFrame(ecDF: DataFrame, dcDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    var gapDF = ecDF.join(dcDF,
      ecDF.col("expCompetencyID") <=> dcDF.col("decCompetencyID") && ecDF.col("expUserID") <=> dcDF.col("decUserID"),
      "leftouter"
    )
    gapDF = gapDF.na.fill(0, Seq("decCompetencyLevel"))  // if null values created during join fill with 0
    gapDF = gapDF.groupBy("expUserID", "expCompetencyID", "expQrgID", "expWorkOrderID")
      .agg(
        max("expCompetencyLevel").alias("expectedLevel"),  // in-case of multiple entries, take max
        max("decCompetencyLevel").alias("declaredLevel")  // in-case of multiple entries, take max
      )
    gapDF = gapDF.select(
      col("expUserID").alias("userID"),
      col("expCompetencyID").alias("competencyID"),
      col("expQrgID").alias("orgID"),
      col("expWorkOrderID").alias("workOrderID"),
      col("expectedLevel"),
      col("declaredLevel")
    )
    gapDF = gapDF.withColumn("competencyGap", expr("expectedLevel - declaredLevel"))

    gapDF.show()
    gapDF.printSchema()
    gapDF
  }

  /**
   * add course data to competency gap data, add user course completion info on top, calculate user competency gap status
   *
   * @param cgDF competency gap data frame
   * @param ccDF course competency data frame
   * @param uccDF user course completion data frame
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedLevel, declaredLevel, competencyGap, completionPercentage, completionStatus)
   */
  def competencyGapCompletionDataFrame(cgDF: DataFrame, ccDF: DataFrame, uccDF: DataFrame): DataFrame = {
    // cgDF - userID, competencyID, orgID, workOrderID, expectedLevel, declaredLevel, competencyGap
    // ccDF - courseID, courseStatus, courseChannel, courseCompetencyID, courseCompetencyLevel
    // uccDF - completionUserID, completionCourseID, completionPercentage, completionStatus

    val cgCourseDF = cgDF.filter("competencyGap > 0")  // for
      .join(ccDF, cgDF.col("competencyID") <=> ccDF.col("courseCompetencyID"), "leftouter")
      .filter("expectedLevel >= courseCompetencyLevel")

    val gapCourseUserStatus = cgCourseDF.join(uccDF,
      cgCourseDF.col("userID") <=> uccDF.col("completionUserID") &&
        cgCourseDF.col("courseID") <=> uccDF.col("completionCourseID"), "leftouter")
      .groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(
        max(col("completionPercentage")))
      .withColumn("completionPercentage", expr("IF(ISNULL(completionPercentage), 0.0, completionPercentage)"))

    var df = cgDF.join(gapCourseUserStatus.as("s"),
      cgDF.col("userID") <=> gapCourseUserStatus.col("s.userID") &&
        cgDF.col("competencyID") <=> gapCourseUserStatus.col("s.competencyID") &&
        cgDF.col("orgID") <=> gapCourseUserStatus.col("s.orgID") &&
        cgDF.col("workOrderID") <=> gapCourseUserStatus.col("s.workOrderID"), "leftouter")
      .select(
        col("userID"), col("competencyID"), col("orgID"), col("workOrderID"),
        col("expectedLevel"), col("declaredLevel"), col("competencyGap"),
        col("s.completionPercentage").alias("completionPercentage")
      )

    df = withCompletionStatusColumn(df)

    df.show()
    df.printSchema()
    df
  }

  /**
   * data frame of course rating summary
   * @return DataFrame(courseID, ratingSummary)
   */
  def courseRatingSummaryDataFrame()(implicit spark: SparkSession): DataFrame = {
    var df = cassandraTableAsDataFrame("sunbird", "ratings_summary")
      .select(
        col("activity_id").alias("courseID"),
        col("sum_of_total_ratings"),
        col("total_number_of_ratings")
      )
    df = df.withColumn("ratingSummary", expr("sum_of_total_ratings / total_number_of_ratings"))

    df.show()
    df.printSchema()
    df
  }

}