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
import org.apache.spark.sql.functions.{col, countDistinct, explode_outer, expr, from_json, lit, max}
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

case class Config(debug: String, broker: String, courseDetailsTopic: String, userCourseProgressTopic: String,
                  fracCompetencyTopic: String, courseCompetencyTopic: String, expectedCompetencyTopic: String,
                  declaredCompetencyTopic: String, competencyGapTopic: String, courseRatingSummaryTopic: String,
                  sparkCassandraConnectionHost: String, sparkDruidRouterHost: String,
                  sparkElasticsearchConnectionHost: String, fracBackendHost: String, cassandraUserKeyspace: String,
                  cassandraCourseKeyspace: String, cassandraHierarchyStoreKeyspace: String, cassandraUserTable: String,
                  cassandraUserContentConsumptionTable: String, cassandraContentHierarchyTable: String,
                  cassandraRatingSummaryTable: String) extends Serializable

/**
 * Model for processing competency metrics
 */
object CompetencyMetricsModel extends IBatchModelTemplate[String, DummyInput, DummyOutput, DummyOutput] with Serializable {

  implicit var debug: Boolean = false

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
   */
  def processCompetencyMetricsData(timestamp: Long, config: Map[String, AnyRef])(implicit spark: SparkSession, sc: SparkContext, fc: FrameworkContext): Unit = {
    // parse model config
    println(config)
    implicit val conf: Config = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    // get course details, dispatch to kafka to be ingested by druid data-source: dashboards-course-details
    val courseDetailsWithCompDF = courseDetailsWithCompetenciesJsonDataFrame()
    val courseDetailsDF = courseDetailsDataFrame(courseDetailsWithCompDF)
    // kafkaDispatch(withTimestamp(courseDetailsDF, timestamp), conf.courseDetailsTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    // val courseCompetencyDF = courseCompetencyDataFrame(courseDetailsWithCompDF)
    // kafkaDispatch(withTimestamp(courseCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get total rating and total number of rating for course rating summary, dispatch to kafka to be ingested by druid data-source: dashboards-course-rating-summary
    // val courseRatingDF = courseRatingSummaryDataFrame()
    // val courseRatingWithDetailsDF = courseRatingSummaryWithDetailsDataFrame(courseRatingDF, courseDetailsDF)
    // kafkaDispatch(withTimestamp(courseRatingWithDetailsDF, timestamp), conf.courseRatingSummaryTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-progress
    // val courseCompletionDF = userCourseCompletionDataFrame()
    // val courseCompletionWithOrgAndNameDF = userCourseCompletionWithDetailsDataFrame(courseCompletionDF, courseDetailsDF)
    // kafkaDispatch(withTimestamp(courseCompletionWithOrgAndNameDF, timestamp), conf.userCourseProgressTopic)

    // get user's expected competency data, dispatch to kafka to be ingested by druid data-source: dashboards-expected-user-competency
    // val expectedCompetencyDF = expectedCompetencyDataFrame()
    // val expectedCompetencyWithCourseCountDF = expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF, courseCompetencyDF)
    // kafkaDispatch(withTimestamp(expectedCompetencyWithCourseCountDF, timestamp), conf.expectedCompetencyTopic)

    // get user's declared competency data, dispatch to kafka to be ingested by druid data-source: dashboards-declared-user-competency
    // val declaredCompetencyDF = declaredCompetencyDataFrame()
    // kafkaDispatch(withTimestamp(declaredCompetencyDF, timestamp), conf.declaredCompetencyTopic)

    // get frac competency data, dispatch to kafka to be ingested by druid data-source: dashboards-frac-competency
    // val fracCompetencyDF = fracCompetencyDataFrame()
    // val fracCompetencyWithCourseCountDF = fracCompetencyWithCourseCountDataFrame(fracCompetencyDF, courseCompetencyDF)
    // val fracCompetencyWithDetailsDF = fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF, expectedCompetencyDF, declaredCompetencyDF)
    // kafkaDispatch(withTimestamp(fracCompetencyWithDetailsDF, timestamp), conf.fracCompetencyTopic)

    // calculate competency gaps, add course completion status, dispatch to kafka to be ingested by druid data-source: dashboards-user-competency-gap
    // val competencyGapDF = competencyGapDataFrame(expectedCompetencyDF, declaredCompetencyDF)
    // val competencyGapWithCompletionDF = competencyGapCompletionDataFrame(competencyGapDF, courseCompetencyDF, courseCompletionDF)  // add course completion status
    // kafkaDispatch(withTimestamp(competencyGapWithCompletionDF, timestamp), conf.competencyGapTopic)
  }

  /* Config functions */

  def getConfig[T](config: Map[String, AnyRef], key: String, default: T = null): T = {
    val path = key.split('.')
    var obj = config
    path.slice(0, path.length - 1).foreach(f => { obj = obj.getOrElse(f, Map()).asInstanceOf[Map[String, AnyRef]] })
    obj.getOrElse(path.last, default).asInstanceOf[T]
  }
  def getConfigModelParam(config: Map[String, AnyRef], key: String): String = getConfig[String](config, key, "")
  def getConfigSideBroker(config: Map[String, AnyRef]): String = getConfig[String](config, "sideOutput.brokerList", "")
  def getConfigSideTopic(config: Map[String, AnyRef], key: String): String = getConfig[String](config, s"sideOutput.topics.${key}", "")
  def parseConfig(config: Map[String, AnyRef]): Config = {
    Config(
      debug = getConfigModelParam(config, "debug"),
      broker = getConfigSideBroker(config),
      courseDetailsTopic = getConfigSideTopic(config, "courseDetails"),
      userCourseProgressTopic = getConfigSideTopic(config, "userCourseProgress"),
      fracCompetencyTopic = getConfigSideTopic(config, "fracCompetency"),
      courseCompetencyTopic = getConfigSideTopic(config, "courseCompetency"),
      expectedCompetencyTopic = getConfigSideTopic(config, "expectedCompetency"),
      declaredCompetencyTopic = getConfigSideTopic(config, "declaredCompetency"),
      competencyGapTopic = getConfigSideTopic(config, "competencyGap"),
      courseRatingSummaryTopic = getConfigSideTopic(config, "courseRatingSummary"),
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      sparkDruidRouterHost = getConfigModelParam(config, "sparkDruidRouterHost"),
      sparkElasticsearchConnectionHost = getConfigModelParam(config, "sparkElasticsearchConnectionHost"),
      fracBackendHost = getConfigModelParam(config, "fracBackendHost"),
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraCourseKeyspace = getConfigModelParam(config, "cassandraCourseKeyspace"),
      cassandraHierarchyStoreKeyspace = getConfigModelParam(config, "cassandraHierarchyStoreKeyspace"),
      cassandraUserTable = getConfigModelParam(config, "cassandraUserTable"),
      cassandraUserContentConsumptionTable = getConfigModelParam(config, "cassandraUserContentConsumptionTable"),
      cassandraContentHierarchyTable = getConfigModelParam(config, "cassandraContentHierarchyTable"),
      cassandraRatingSummaryTable = getConfigModelParam(config, "cassandraRatingSummaryTable")
    )
  }

  /* Util functions */
  def show(df: DataFrame): Unit = {
    if (debug) df.show()
    df.printSchema()
  }

  def withTimestamp(df: DataFrame, timestamp: Long): DataFrame = {
    df.withColumn("timestamp", lit(timestamp))
  }

  def kafkaDispatch(data: DataFrame, topic: String)(implicit sc: SparkContext, fc: FrameworkContext, conf: Config): Unit = {
    if (topic == "") {
      println("ERROR: topic is blank, skipping kafka dispatch")
    } else if (conf.broker == "") {
      println("ERROR: broker list is blank, skipping kafka dispatch")
    } else {
      KafkaDispatcher.dispatch(Map("brokerList" -> conf.broker, "topic" -> topic), data.toJSON.rdd)
    }
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
   * completionPercentage   completionStatus    IDI status
   * NULL                   not-enrolled        not-started
   * 0.0                    enrolled            not-started
   * 0.0 < % < 10.0         started             enrolled
   * 10.0 <= % < 100.0      in-progress         in-progress
   * 100.0                  completed           completed
   * @param df data frame with completionPercentage column
   * @return df with completionStatus column
   */
  def withCompletionStatusColumn(df: DataFrame): DataFrame = {
    val caseExpression = "CASE WHEN ISNULL(completionPercentage) THEN 'not-enrolled' WHEN completionPercentage == 0.0 THEN 'enrolled' WHEN completionPercentage < 10.0 THEN 'started' WHEN completionPercentage < 100.0 THEN 'in-progress' ELSE 'completed' END"
    df.withColumn("completionStatus", expr(caseExpression))
  }

  /* Data processing functions */

  /**
   * Distinct live course ids from elastic search api
   * need to do this because otherwise we will have to parse all json records in cassandra to filter live ones
   * @return DataFrame(id)
   */
  def liveCourseDataFrame()(implicit spark: SparkSession, conf: Config): DataFrame = {
    val result = elasticSearchCourseAPI(conf.sparkElasticsearchConnectionHost)

    var df = dataFrameFromJSONString(result)
    df = df.select(explode_outer(col("hits.hits")).alias("course"))
    df = df.select(col("course._source.identifier").alias("id")).distinct()

    show(df)
    df
  }

  /* schema definitions for courseDetailsDataFrame */
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("competencies_v3", StringType, nullable = true)
  ))
  /**
   * course details with competencies json from cassandra dev_hierarchy_store:content_hierarchy
   * @return DataFrame(courseID, courseName, courseStatus, courseOrgID, competenciesJson)
   */
  def courseDetailsWithCompetenciesJsonDataFrame()(implicit spark: SparkSession, conf: Config): DataFrame = {
    val rawCourseDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
    val liveCourseIDsDF = liveCourseDataFrame()  // get ids for live courses from es api

    // inner join so that we only retain live courses
    var df = liveCourseIDsDF.join(rawCourseDF,
      liveCourseIDsDF.col("id") <=> rawCourseDF.col("identifier"), "inner")
      .filter(col("hierarchy").isNotNull)

    df = df.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
    df = df.select(
      col("id").alias("courseID"),
      col("data.name").alias("courseName"),
      col("data.status").alias("courseStatus"),
      col("data.channel").alias("courseOrgID"),
      col("data.competencies_v3").alias("competenciesJson")
    )

    show(df)
    df
  }

  /**
   * course details without competencies json
   * @param courseDetailsWithCompDF course details with competencies json
   * @return DataFrame(courseID, courseName, courseStatus, courseOrgID)
   */
  def courseDetailsDataFrame(courseDetailsWithCompDF: DataFrame): DataFrame = {
    val df = courseDetailsWithCompDF.drop("competenciesJson")

    show(df)
    df
  }

  /* schema definitions for courseCompetencyDataFrame */
  val courseCompetenciesSchema: ArrayType = ArrayType(StructType(Seq(
    StructField("id",  StringType, nullable = true),
    StructField("selectedLevelLevel",  StringType, nullable = true)
  )))
  /**
   * course competency mapping data from cassandra dev_hierarchy_store:content_hierarchy
   * @param courseDetailsWithCompDF course details with competencies json
   * @return DataFrame(courseID, courseName, courseStatus, courseOrgID, competencyID, competencyLevel)
   */
  def courseCompetencyDataFrame(courseDetailsWithCompDF: DataFrame)(implicit spark: SparkSession, conf: Config): DataFrame = {
    var df = courseDetailsWithCompDF.withColumn("competencies", from_json(col("competenciesJson"), courseCompetenciesSchema))

    df = df.select(
      col("courseID"), col("courseName"), col("courseStatus"), col("courseOrgID"),
      explode_outer(col("competencies")).alias("competency")
    ).filter(col("competency").isNotNull)

    df = df.withColumn("competencyLevel", expr("TRIM(competency.selectedLevelLevel)"))
    df = df.withColumn("competencyLevel",
      expr("IF(competencyLevel RLIKE '[0-9]+', CAST(REGEXP_EXTRACT(competencyLevel, '[0-9]+', 0) AS INTEGER), 1)"))

    df = df.select(
      col("courseID"), col("courseName"), col("courseStatus"), col("courseOrgID"),
      col("competency.id").alias("competencyID"),
      col("competencyLevel")
    )

    show(df)
    df
  }

  /**
   * data frame of course rating summary
   * @return DataFrame(courseID, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
   */
  def courseRatingSummaryDataFrame()(implicit spark: SparkSession, conf: Config): DataFrame = {
    val df = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraRatingSummaryTable)
      .where(expr("LOWER(activitytype) == 'course' AND total_number_of_ratings > 0"))
      .withColumn("ratingAverage", expr("sum_of_total_ratings / total_number_of_ratings"))
      .select(
        col("activityid").alias("courseID"),
        col("sum_of_total_ratings").alias("ratingSum"),
        col("total_number_of_ratings").alias("ratingCount"),
        col("ratingAverage"),
        col("totalcount1stars").alias("count1Star"),
        col("totalcount2stars").alias("count2Star"),
        col("totalcount3stars").alias("count3Star"),
        col("totalcount4stars").alias("count4Star"),
        col("totalcount5stars").alias("count5Star")
      )

    show(df)
    df
  }

  /**
   * attach courseName, courseStatus, courseOrgID to course rating summary data frame
   * @param courseRatingDF course rating summary data frame
   * @param courseDetailsDF course details data frame
   * @return DataFrame(courseID, courseName, courseStatus, courseOrgID, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
   */
  def courseRatingSummaryWithDetailsDataFrame(courseRatingDF: DataFrame, courseDetailsDF: DataFrame)(implicit spark: SparkSession, conf: Config): DataFrame = {
    // courseRatingDF = DataFrame(courseID, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
    // courseDetailsDF = DataFrame(courseID, courseName, courseStatus, courseOrgID)
    val df = courseRatingDF.join(courseDetailsDF, Seq("courseID"), "leftouter")

    show(df)
    df
  }

  /**
   * data frame of user course completion percentage
   * @return DataFrame(userID, courseID, completionPercentage, completionStatus)
   */
  def userCourseCompletionDataFrame()(implicit spark: SparkSession, conf: Config): DataFrame = {
    var df = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserContentConsumptionTable)
      .select(
        col("userid").alias("userID"),
        col("courseid").alias("courseID"),
        col("completionpercentage").alias("completionPercentage")
      )
    df = withCompletionStatusColumn(df)

    show(df)
    df
  }

  /**
   * attach courseName, courseStatus, courseOrgID to course rating summary data frame
   * @param courseCompletionDF user course completion data frame
   * @param courseDetailsDF course details data frame
   * @return DataFrame(userID, courseID, courseName, courseStatus, courseOrgID, completionPercentage, completionStatus)
   */
  def userCourseCompletionWithDetailsDataFrame(courseCompletionDF: DataFrame, courseDetailsDF: DataFrame)(implicit spark: SparkSession, conf: Config): DataFrame = {
    // courseCompletionDF = DataFrame(userID, courseID, completionPercentage, completionStatus)
    // courseDetailsDF = DataFrame(courseID, courseName, courseStatus, courseOrgID)
    val df = courseCompletionDF.join(courseDetailsDF, Seq("courseID"), "leftouter")

    show(df)
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them from druid
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   */
  def expectedCompetencyDataFrame()(implicit spark: SparkSession, conf: Config) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS orgID, edata_cb_data_wa_id AS workOrderID, edata_cb_data_wa_userId AS userID, edata_cb_data_wa_competency_id AS competencyID, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS competencyLevel FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    val result = druidSQLAPI(query, conf.sparkDruidRouterHost)

    val df = dataFrameFromJSONString(result)
      .filter(col("competencyID").isNotNull && col("competencyLevel").notEqual(0))
      .withColumn("expectedCompetencyLevel", expr("CAST(competencyLevel as INTEGER)"))  // Important to cast as integer otherwise a cast will fail later on

    show(df)
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them, including live course count
   * @param expectedCompetencyDF expected competency data frame
   * @param courseCompetencyDF course competency data frame
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel, liveCourseCount)
   */
  def expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF: DataFrame, courseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: Config) : DataFrame = {
    // expectedCompetencyDF = DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
    // courseCompetencyDF = DataFrame(courseID, courseName, courseStatus, courseOrgID, competencyID, competencyLevel)

    // live course count DF
    val liveCourseCountDF = expectedCompetencyDF.join(courseCompetencyDF, Seq("competencyID"), "leftouter")
      .where(expr("expectedCompetencyLevel <= competencyLevel"))
      .groupBy("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = expectedCompetencyDF.join(liveCourseCountDF, Seq("orgID", "workOrderID", "userID", "competencyID", "expectedCompetencyLevel"), "leftouter")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
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
   * @return DataFrame(userID, competencyID, declaredCompetencyLevel)
   */
  def declaredCompetencyDataFrame()(implicit spark: SparkSession, conf: Config) : DataFrame = {
    val userdata = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)

    val df = userdata.where(col("profiledetails").isNotNull)
      .select("userid", "profiledetails")
      .withColumn("profile", from_json(col("profiledetails"), profileDetailsSchema))
      .select(col("userid"), explode_outer(col("profile.competencies")).alias("competency"))
      .where(col("competency").isNotNull && col("competency.id").isNotNull)
      .select(
        col("userid").alias("userID"),
        col("competency.id").alias("competencyID"),
        col("competency.competencySelfAttestedLevel").alias("declaredCompetencyLevel")
      )
      .na.fill(1, Seq("declaredCompetencyLevel"))  // if competency is listed without a level assume level 1

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api
   * @return DataFrame(competencyID, competencyName, competencyStatus)
   */
  def fracCompetencyDataFrame()(implicit spark: SparkSession, conf: Config): DataFrame = {
    val result = fracCompetencyAPI(conf.fracBackendHost)
    val df = dataFrameFromJSONString(result)
      .select(explode_outer(col("data.getAllCompetencies")).alias("competency"))
      .select(
        col("competency.id").alias("competencyID"),
        col("competency.name").alias("competencyName"),
        col("competency.status").alias("competencyStatus")
      )

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including live course count
   * @param fracCompetencyDF frac competency data frame
   * @param courseCompetencyDF course competency data frame
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
   */
  def fracCompetencyWithCourseCountDataFrame(fracCompetencyDF: DataFrame, courseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: Config) : DataFrame = {
    // fracCompetencyDF = DataFrame(competencyID, competencyName, competencyStatus)
    // courseCompetencyDF = DataFrame(courseID, courseName, courseStatus, courseOrgID, competencyID, competencyLevel)

    // live course count DF
    val liveCourseCountDF = fracCompetencyDF.join(courseCompetencyDF, Seq("competencyID"), "leftouter")
      .filter(col("courseID").isNotNull)
      .groupBy("competencyID", "competencyName", "competencyStatus")
      .agg(countDistinct("courseID").alias("liveCourseCount"))

    val df = fracCompetencyDF.join(liveCourseCountDF, Seq("competencyID", "competencyName", "competencyStatus"), "leftouter")
      .na.fill(0, Seq("liveCourseCount"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including officer count
   * @param fracCompetencyWithCourseCountDF frac competency data frame with live course count
   * @param expectedCompetencyDF expected competency data frame
   * @param declaredCompetencyDF declared  competency data frame
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount, officerCountExpected, officerCountDeclared)
   */
  def fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF: DataFrame, expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: Config) : DataFrame = {
    // fracCompetencyWithCourseCountDF = DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
    // expectedCompetencyDF = DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
    // declaredCompetencyDF = DataFrame(userID, competencyID, declaredCompetencyLevel)

    // add expected officer count
    val fcExpectedCountDF = fracCompetencyWithCourseCountDF.join(expectedCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus", "liveCourseCount")
      .agg(countDistinct("userID").alias("officerCountExpected"))

    // add declared officer count
    val df = fcExpectedCountDF.join(declaredCompetencyDF, Seq("competencyID"), "leftouter")
      .groupBy("competencyID", "competencyName", "competencyStatus", "liveCourseCount", "officerCountExpected")
      .agg(countDistinct("userID").alias("officerCountDeclared"))

    show(df)
    df
  }

  /**
   * Calculates user's competency gaps
   * @param expectedCompetencyDF expected competency data frame
   * @param declaredCompetencyDF declared competency data frame
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap)
   */
  def competencyGapDataFrame(expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // expectedCompetencyDF: DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
    // declaredCompetencyDF: DataFrame(userID, competencyID, declaredCompetencyLevel)

    var df = expectedCompetencyDF.join(declaredCompetencyDF, Seq("competencyID", "userID"), "leftouter")
    df = df.na.fill(0, Seq("declaredCompetencyLevel"))  // if null values created during join fill with 0
    df = df.groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(
        max("expectedCompetencyLevel").alias("expectedCompetencyLevel"),  // in-case of multiple entries, take max
        max("declaredCompetencyLevel").alias("declaredCompetencyLevel")  // in-case of multiple entries, take max
      )
    df = df.withColumn("competencyGap", expr("expectedCompetencyLevel - declaredCompetencyLevel"))

    show(df)
    df
  }

  /**
   * add course data to competency gap data, add user course completion info on top, calculate user competency gap status
   *
   * @param competencyGapDF competency gap data frame
   * @param courseCompetencyDF course competency data frame
   * @param courseCompletionDF user course completion data frame
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, completionPercentage, completionStatus)
   */
  def competencyGapCompletionDataFrame(competencyGapDF: DataFrame, courseCompetencyDF: DataFrame, courseCompletionDF: DataFrame): DataFrame = {
    // competencyGapDF - userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap
    // courseCompetencyDF - courseID, courseName, courseStatus, courseOrgID, competencyID, competencyLevel
    // courseCompletionDF - userID, courseID, courseName, courseStatus, courseOrgID, completionPercentage, completionStatus

    // userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, courseID, courseName, courseStatus, courseOrgID, competencyLevel
    val cgCourseDF = competencyGapDF.filter("competencyGap > 0")
      .join(courseCompetencyDF, Seq("competencyID"), "leftouter")
      .filter("expectedCompetencyLevel >= competencyLevel")

    // userID, competencyID, orgID, workOrderID, completionPercentage
    val gapCourseUserStatus = cgCourseDF.join(courseCompletionDF, Seq("userID", "courseID"), "leftouter")
      .groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(max(col("completionPercentage")).alias("completionPercentage"))
      .withColumn("completionPercentage", expr("IF(ISNULL(completionPercentage), 0.0, completionPercentage)"))

    var df = competencyGapDF.join(gapCourseUserStatus, Seq("userID", "competencyID", "orgID", "workOrderID"), "leftouter")

    df = withCompletionStatusColumn(df)

    show(df)
    df
  }

}