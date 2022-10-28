package org.ekstep.analytics.dashboard

import java.io.Serializable
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, countDistinct, explode_outer, expr, from_json, last, max, udf}
import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StringType, StructField, StructType}
import org.ekstep.analytics.framework._

import scala.util.matching.Regex
import DashboardUtil._

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

case class CMDummyInput(timestamp: Long) extends AlgoInput  // no input, there are multiple sources to query
case class CMDummyOutput() extends Output with AlgoOutput  // no output as we take care of kafka dispatches ourself

case class CMConfig(
                     debug: String, broker: String, compression: String,
                     redisHost: String, redisPort: Int, redisDB: Int,
                     allCourseTopic: String, allResourceTopic: String,
                     courseDetailsTopic: String, userCourseProgressTopic: String,
                     fracCompetencyTopic: String, courseCompetencyTopic: String, expectedCompetencyTopic: String,
                     declaredCompetencyTopic: String, competencyGapTopic: String,
                     sparkCassandraConnectionHost: String, sparkDruidRouterHost: String,
                     sparkElasticsearchConnectionHost: String, fracBackendHost: String, cassandraUserKeyspace: String,
                     cassandraCourseKeyspace: String, cassandraHierarchyStoreKeyspace: String,
                     cassandraUserTable: String, cassandraOrgTable: String,
                     cassandraUserEnrolmentsTable: String, cassandraContentHierarchyTable: String,
                     cassandraRatingSummaryTable: String,
                     redisExpectedUserCompetencyCount: String, redisDeclaredUserCompetencyCount: String,
                     redisUserCompetencyDeclarationRate: String, redisOrgCompetencyDeclarationRate: String,
                     redisUserCompetencyGapCount: String, redisUserCourseEnrollmentCount: String,
                     redisUserCompetencyGapEnrollmentRate: String, redisOrgCompetencyGapEnrollmentRate: String,
                     redisUserCourseCompletionCount: String, redisUserCompetencyGapClosedCount: String,
                     redisUserCompetencyGapClosedRate: String, redisOrgCompetencyGapClosedRate: String
                   ) extends DashboardConfig

/**
 * Model for processing competency metrics
 */
object CompetencyMetricsModel extends IBatchModelTemplate[String, CMDummyInput, CMDummyOutput, CMDummyOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.dashboard.CompetencyMetricsModel"
  override def name() = "CompetencyMetricsModel"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CMDummyInput] = {
    // we want this call to happen only once, so that timestamp is consistent for all data points
    val executionTime = System.currentTimeMillis()
    sc.parallelize(Seq(CMDummyInput(executionTime)))
  }

  override def algorithm(data: RDD[CMDummyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CMDummyOutput] = {
    val timestamp = data.first().timestamp  // extract timestamp from input
    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
    processCompetencyMetricsData(timestamp, config)
    sc.parallelize(Seq())  // return empty rdd
  }

  override def postProcess(data: RDD[CMDummyOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CMDummyOutput] = {
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
    implicit val conf: CMConfig = parseConfig(config)
    if (conf.debug == "true") debug = true // set debug to true if explicitly specified in the config

    // get all courses with name, status and org, dispatch to kafka to be ingested by druid data-source: dashboards-courses
    val allCourseDF = allCourseDataFrame()
    kafkaDispatch(withTimestamp(allCourseDF, timestamp), conf.allCourseTopic)

    // get all resources with status and org, dispatch to kafka to be ingested by druid data-source: dashboards-resources
//    val liveResourceDF = liveResourceDataFrame()
//    kafkaDispatch(withTimestamp(liveResourceDF, timestamp), conf.allResourceTopic)

    val liveCourseIDsDF = liveCourseDataFrame(allCourseDF)  // get ids for live courses from es api

    // get course details, attach rating info, dispatch to kafka to be ingested by druid data-source: dashboards-course-details
    val courseDetailsWithCompDF = courseDetailsWithCompetenciesJsonDataFrame(liveCourseIDsDF)
    val courseDetailsDF = courseDetailsDataFrame(courseDetailsWithCompDF)
    val courseRatingDF = courseRatingSummaryDataFrame()
    val courseDetailsWithRatingDF = courseDetailsWithRatingDataFrame(courseDetailsDF, courseRatingDF)
    kafkaDispatch(withTimestamp(courseDetailsWithRatingDF, timestamp), conf.courseDetailsTopic)

    // get course competency mapping data, dispatch to kafka to be ingested by druid data-source: dashboards-course-competency
    val courseCompetencyDF = courseCompetencyDataFrame(courseDetailsWithCompDF)
    kafkaDispatch(withTimestamp(courseCompetencyDF, timestamp), conf.courseCompetencyTopic)

    // get course completion data, dispatch to kafka to be ingested by druid data-source: dashboards-user-course-progress
    val courseCompletionWithDetailsDF = userCourseCompletionWithDetailsDataFrame(courseDetailsDF)
    kafkaDispatch(withTimestamp(courseCompletionWithDetailsDF, timestamp), conf.userCourseProgressTopic)

    // get user's expected competency data, dispatch to kafka to be ingested by druid data-source: dashboards-expected-user-competency
    val expectedCompetencyDF = expectedCompetencyDataFrame()
    val expectedCompetencyWithCourseCountDF = expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF, courseCompetencyDF)
    kafkaDispatch(withTimestamp(expectedCompetencyWithCourseCountDF, timestamp), conf.expectedCompetencyTopic)

    // get user's declared competency data, dispatch to kafka to be ingested by druid data-source: dashboards-declared-user-competency
    val declaredCompetencyDF = declaredCompetencyDataFrame()
    kafkaDispatch(withTimestamp(declaredCompetencyDF, timestamp), conf.declaredCompetencyTopic)

    // get frac competency data, dispatch to kafka to be ingested by druid data-source: dashboards-frac-competency
    val fracCompetencyDF = fracCompetencyDataFrame()
    val fracCompetencyWithCourseCountDF = fracCompetencyWithCourseCountDataFrame(fracCompetencyDF, courseCompetencyDF)
    val fracCompetencyWithDetailsDF = fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF, expectedCompetencyDF, declaredCompetencyDF)
    kafkaDispatch(withTimestamp(fracCompetencyWithDetailsDF, timestamp), conf.fracCompetencyTopic)

    // calculate competency gaps, add course completion status, dispatch to kafka to be ingested by druid data-source: dashboards-user-competency-gap
    val competencyGapDF = competencyGapDataFrame(expectedCompetencyDF, declaredCompetencyDF)
    val competencyGapWithCompletionDF = competencyGapCompletionDataFrame(competencyGapDF, courseCompetencyDF, courseCompletionWithDetailsDF)  // add course completion status
    kafkaDispatch(withTimestamp(competencyGapWithCompletionDF, timestamp), conf.competencyGapTopic)

    // officer dashboard metrics redis dispatch
    // OL01 - user: expected_competency_count
    val userExpectedCompetencyCountDF = expectedCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userExpectedCompetencyCountDF, "OL01")
    redisDispatchDataFrame[Long](conf.redisExpectedUserCompetencyCount, userExpectedCompetencyCountDF, "userID", "count")

    // OL02 - user: declared_competency_count
    val userDeclaredCompetencyCountDF = declaredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"))
    show(userDeclaredCompetencyCountDF, "OL02")
    redisDispatchDataFrame[Long](conf.redisDeclaredUserCompetencyCount, userDeclaredCompetencyCountDF, "userID", "count")

    // OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
    val coveredCompetencyDF = expectedCompetencyDF.join(declaredCompetencyDF, Seq("userID", "competencyID"), "leftouter")
      .na.fill(0, Seq("declaredCompetencyLevel"))
      .where(expr("declaredCompetencyLevel >= expectedCompetencyLevel"))
    val userCoveredCompetencyCountDF = coveredCompetencyDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("coveredCount"))
    val userCompetencyCoverRateDF = userExpectedCompetencyCountDF.join(userCoveredCompetencyCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("coveredCount"))
      .withColumn("rate", expr("coveredCount / count"))
    show(userCompetencyCoverRateDF, "OL03")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyDeclarationRate, userCompetencyCoverRateDF, "userID", "rate")

    // OL04 - mdo: average_competency_declaration_rate
    val orgCompetencyAvgCoverRateDF = userCompetencyCoverRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyAvgCoverRateDF, "OL04")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyDeclarationRate, orgCompetencyAvgCoverRateDF, "orgID", "rate")

    // OL05 - user: competency gap count
    val userCompetencyGapDF = competencyGapDF.where(expr("competencyGap > 0"))
    val userCompetencyGapCountDF = userCompetencyGapDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("count"), last("orgID").alias("orgID"))
    show(userCompetencyGapCountDF, "OL05")
    redisDispatchDataFrame[Long](conf.redisUserCompetencyGapCount, userCompetencyGapCountDF, "userID", "count")

    // OL06 - user: enrolled cbp count (IMPORTANT: excluding completed courses)
    val userCourseEnrolledDF = courseCompletionWithDetailsDF.where(expr("completionStatus in ('started', 'in-progress')"))
    val userCourseEnrolledCountDF = userCourseEnrolledDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseEnrolledCountDF, "OL06")
    redisDispatchDataFrame[Long](conf.redisUserCourseEnrollmentCount, userCourseEnrolledCountDF, "userID", "count")

    // OL08 - user: competency gaps enrolled percentage (IMPORTANT: excluding completed ones)
    val userCompetencyGapEnrolledDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus in ('started', 'in-progress')"))
    val userCompetencyGapEnrolledCountDF = userCompetencyGapEnrolledDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("enrolledCount"))
    val userCompetencyGapEnrolledRateDF = userCompetencyGapCountDF.join(userCompetencyGapEnrolledCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("enrolledCount"))
      .withColumn("rate", expr("enrolledCount / count"))
    show(userCompetencyGapEnrolledRateDF, "OL08")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyGapEnrollmentRate, userCompetencyGapEnrolledRateDF, "userID", "rate")

    // OL09 - mdo: average competency gaps enrolled percentage
    val orgCompetencyGapAvgEnrolledRateDF = userCompetencyGapEnrolledRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgEnrolledRateDF, "OL09")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyGapEnrollmentRate, orgCompetencyGapAvgEnrolledRateDF, "orgID", "rate")

    // OL10 - user: completed cbp count
    val userCourseCompletedDF = courseCompletionWithDetailsDF.where(expr("completionStatus = 'completed'"))
    val userCourseCompletedCountDF = userCourseCompletedDF.groupBy("userID").agg(
      countDistinct("courseID").alias("count"))
    show(userCourseCompletedCountDF, "OL10")
    redisDispatchDataFrame[Long](conf.redisUserCourseCompletionCount, userCourseCompletedCountDF, "userID", "count")

    // OL11 - user: competency gap closed count
    val userCompetencyGapClosedDF = competencyGapWithCompletionDF.where(expr("competencyGap > 0 AND completionStatus = 'completed'"))
    val userCompetencyGapClosedCountDF = userCompetencyGapClosedDF.groupBy("userID").agg(
      countDistinct("competencyID").alias("closedCount"))
    show(userCompetencyGapClosedCountDF, "OL11")
    redisDispatchDataFrame[Long](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedCountDF, "userID", "closedCount")

    // OL12 - user: competency gap closed percent
    val userCompetencyGapClosedRateDF = userCompetencyGapCountDF.join(userCompetencyGapClosedCountDF, Seq("userID"), "leftouter")
      .na.fill(0, Seq("closedCount"))
      .withColumn("rate", expr("closedCount / count"))
    show(userCompetencyGapClosedRateDF,  "OL12")
    redisDispatchDataFrame[Double](conf.redisUserCompetencyGapClosedCount, userCompetencyGapClosedRateDF, "userID", "rate")

    // OL13 - mdo: avg competency gap closed percent
    val orgCompetencyGapAvgClosedRateDF = userCompetencyGapClosedRateDF.groupBy("orgID")
      .agg(avg("rate").alias("rate"))
    show(orgCompetencyGapAvgClosedRateDF, "OL13")
    redisDispatchDataFrame[Double](conf.redisOrgCompetencyGapClosedRate, orgCompetencyGapAvgClosedRateDF, "orgID", "rate")

    closeRedisConnect()
  }

  /**
   * OL01 - user: expected_competency_count
   * OL02 - user: declared_competency_count
   * OL03 - user: (declared_competency intersection expected_competency).count / expected_competency_count
   * OL04 - mdo: average_competency_declaration_rate
   * OL05 - user: competency gap count
   * OL06 - user: enrolled cbp count
   * OL08 - user: competency gaps enrolled percentage
   * OL09 - mdo: average competency gaps enrolled percentage
   * OL10 - user: completed cbp count
   * OL11 - user: competency gap closed count
   * OL12 - user: competency gap closed percent
   * OL13 - mdo: avg competency gap closed percent
   */


  def parseConfig(config: Map[String, AnyRef]): CMConfig = {
    CMConfig(
      debug = getConfigModelParam(config, "debug"),
      broker = getConfigSideBroker(config),
      compression = getConfigSideBrokerCompression(config),
      redisHost = getConfigModelParam(config, "redisHost"),
      redisPort = getConfigModelParam(config, "redisPort").toInt,
      redisDB = getConfigModelParam(config, "redisDB").toInt,
      allCourseTopic = getConfigSideTopic(config, "allCourses"),
      allResourceTopic = getConfigSideTopic(config, "allResources"),
      courseDetailsTopic = getConfigSideTopic(config, "courseDetails"),
      userCourseProgressTopic = getConfigSideTopic(config, "userCourseProgress"),
      fracCompetencyTopic = getConfigSideTopic(config, "fracCompetency"),
      courseCompetencyTopic = getConfigSideTopic(config, "courseCompetency"),
      expectedCompetencyTopic = getConfigSideTopic(config, "expectedCompetency"),
      declaredCompetencyTopic = getConfigSideTopic(config, "declaredCompetency"),
      competencyGapTopic = getConfigSideTopic(config, "competencyGap"),
      sparkCassandraConnectionHost = getConfigModelParam(config, "sparkCassandraConnectionHost"),
      sparkDruidRouterHost = getConfigModelParam(config, "sparkDruidRouterHost"),
      sparkElasticsearchConnectionHost = getConfigModelParam(config, "sparkElasticsearchConnectionHost"),
      fracBackendHost = getConfigModelParam(config, "fracBackendHost"),
      cassandraUserKeyspace = getConfigModelParam(config, "cassandraUserKeyspace"),
      cassandraCourseKeyspace = getConfigModelParam(config, "cassandraCourseKeyspace"),
      cassandraHierarchyStoreKeyspace = getConfigModelParam(config, "cassandraHierarchyStoreKeyspace"),
      cassandraUserTable = getConfigModelParam(config, "cassandraUserTable"),
      cassandraOrgTable = getConfigModelParam(config, "cassandraOrgTable"),
      cassandraUserEnrolmentsTable = getConfigModelParam(config, "cassandraUserEnrolmentsTable"),
      cassandraContentHierarchyTable = getConfigModelParam(config, "cassandraContentHierarchyTable"),
      cassandraRatingSummaryTable = getConfigModelParam(config, "cassandraRatingSummaryTable"),
      redisExpectedUserCompetencyCount = "dashboard_expected_user_competency_count",
      redisDeclaredUserCompetencyCount = "dashboard_declared_user_competency_count",
      redisUserCompetencyDeclarationRate = "dashboard_user_competency_declaration_rate",
      redisOrgCompetencyDeclarationRate = "dashboard_org_competency_declaration_rate",
      redisUserCompetencyGapCount = "dashboard_user_competency_gap_count",
      redisUserCourseEnrollmentCount = "dashboard_user_course_enrollment_count",
      redisUserCompetencyGapEnrollmentRate = "dashboard_user_competency_gap_enrollment_rate",
      redisOrgCompetencyGapEnrollmentRate = "dashboard_org_competency_gap_enrollment_rate",
      redisUserCourseCompletionCount = "dashboard_user_course_completion_count",
      redisUserCompetencyGapClosedCount = "dashboard_user_competency_gap_closed_count",
      redisUserCompetencyGapClosedRate = "dashboard_user_competency_gap_closed_rate",
      redisOrgCompetencyGapClosedRate = "dashboard_org_competency_gap_closed_rate"
    )
  }

  def elasticSearchCourseDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val query = """{"_source":["identifier","name","primaryCategory","status","reviewStatus","channel","competencies"],"query":{"bool":{"must":[{"match":{"primaryCategory":"Course"}}]}}}"""
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel")
    elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)
  }

  def elasticSearchLiveResourceDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val query = """{"_source":["identifier","name","primaryCategory","status","reviewStatus","channel"],"query":{"bool":{"must":[{"match":{"status":"Live"}},{"match":{"primaryCategory":"Learning Resource"}}]}}}"""
    val fields = Seq("identifier", "name", "primaryCategory", "status", "reviewStatus", "channel")
    elasticSearchDataFrame(conf.sparkElasticsearchConnectionHost, "compositesearch", query, fields)
  }

  def fracCompetencyAPI(host: String): String = {
    val url = s"https://${host}/graphql"
    val requestBody = """{"operationName":"filterCompetencies","variables":{"cod":[],"competencyType":[],"competencyArea":[],"competencySector":[]},"query":"query filterCompetencies($cod: [String], $competencyType: [String], $competencyArea: [String], $competencySector: [String]) {\n  getAllCompetencies(\n    cod: $cod\n    competencyType: $competencyType\n    competencyArea: $competencyArea\n    competencySector: $competencySector\n  ) {\n    name\n    id\n    description\n    status\n    source\n    additionalProperties {\n      cod\n      competencyType\n      competencyArea\n      competencySector\n      __typename\n    }\n    __typename\n  }\n}\n"}"""
    api("POST", url, requestBody)
  }

  def fracCompetencyDFOption(host: String)(implicit spark: SparkSession): Option[DataFrame] = {
    var result = fracCompetencyAPI(host)
    result = result.trim()
    // return empty data frame if result is an empty string
    if (result == "") {
      println(s"ERROR: fracCompetencyAPI returned empty string")
      return None
    }
    val df = dataFrameFromJSONString(result)  // parse json string
    if (df.isEmpty) {
      println(s"ERROR: druidSQLAPI json parse result is empty")
      return None
    }
    // return empty data frame if there is an `errors` field in the json
    if (hasColumn(df, "errors")) {
      println(s"ERROR: fracCompetencyAPI returned error response, response=${result}")
      return None
    }
    // now that error handling is done, proceed with business as usual
    Some(df)
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
   * All courses from elastic search api
   * @return DataFrame(courseID, courseName, courseStatus, courseReviewStatus, courseOrgID)
   */
  val allCourseSchema: StructType = StructType(Seq(
    StructField("courseID",  StringType, nullable = true),
    StructField("courseName",  StringType, nullable = true),
    StructField("courseStatus",  StringType, nullable = true),
    StructField("courseReviewStatus",  StringType, nullable = true),
    StructField("courseOrgID",  StringType, nullable = true)
  ))
  def allCourseDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = elasticSearchCourseDataFrame()

    // now that error handling is done, proceed with business as usual
    df = df.select(
      col("identifier").alias("courseID"),
      col("name").alias("courseName"),
      col("status").alias("courseStatus"),
      col("reviewStatus").alias("courseReviewStatus"),
      col("channel").alias("courseOrgID")
    )
    df = df.dropDuplicates("courseID")

    show(df)
    df
  }

  /**
   * Live resources from elastic search api
   * @return DataFrame(resourceID, resourceStatus, resourceReviewStatus, resourceOrgID)
   */
  val allResourceSchema: StructType = StructType(Seq(
    StructField("resourceID",  StringType, nullable = true),
    StructField("resourceStatus",  StringType, nullable = true),
    StructField("resourceReviewStatus",  StringType, nullable = true),
    StructField("resourceOrgID",  StringType, nullable = true)
  ))
  def liveResourceDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = elasticSearchLiveResourceDataFrame()

    // now that error handling is done, proceed with business as usual
    df = df.select(
      col("identifier").alias("resourceID"),
      col("status").alias("resourceStatus"),
      col("reviewStatus").alias("resourceReviewStatus"),
      col("channel").alias("resourceOrgID")
    )
    df = df.dropDuplicates("resourceID")

    show(df)
    df
  }

  // only live course ids
  val liveCourseSchema: StructType = StructType(Seq(
    StructField("id",  StringType, nullable = true)
  ))
  def liveCourseDataFrame(allCourseDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val df = allCourseDF.where(col("courseStatus").equalTo("Live")).select(col("courseID").alias("id")).distinct()

    show(df)
    df
  }

  /* schema definitions for courseDetailsDataFrame */
  val courseHierarchySchema: StructType = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("duration", StringType, nullable = true),
    StructField("leafNodesCount", IntegerType, nullable = true),
    StructField("competencies_v3", StringType, nullable = true)
  ))
  /**
   * course details with competencies json from cassandra dev_hierarchy_store:content_hierarchy
   * @return DataFrame(courseID, courseName, courseStatus, courseDuration, courseResourceCount, courseOrgID, competenciesJson)
   */
  def courseDetailsWithCompetenciesJsonDataFrame(liveCourseIDsDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    val rawCourseDF = cassandraTableAsDataFrame(conf.cassandraHierarchyStoreKeyspace, conf.cassandraContentHierarchyTable)
      .select(col("identifier").alias("id"), col("hierarchy"))

    // inner join so that we only retain live courses
    var df = liveCourseIDsDF.join(rawCourseDF, Seq("id"), "inner")
    df = df.filter(col("hierarchy").isNotNull)
    df = df.withColumn("data", from_json(col("hierarchy"), courseHierarchySchema))
    df = df.select(
      col("id").alias("courseID"),
      col("data.name").alias("courseName"),
      col("data.status").alias("courseStatus"),
      col("data.duration").cast(FloatType).alias("courseDuration"),
      col("data.leafNodesCount").alias("courseResourceCount"),
      col("data.channel").alias("courseOrgID"),
      col("data.competencies_v3").alias("competenciesJson")
    )
    df = df.na.fill(0.0, Seq("courseDuration")).na.fill(0, Seq("courseResourceCount"))

    show(df, "courseDetailsWithCompetenciesJsonDataFrame() = (courseID, courseName, courseStatus, courseDuration, courseResourceCount, courseOrgID, competenciesJson)")
    df
  }

  /**
   * course details without competencies json
   * @param courseDetailsWithCompDF course details with competencies json
   * @return DataFrame(courseID, courseName, courseStatus, courseDuration, courseResourceCount, courseOrgID)
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
  def courseCompetencyDataFrame(courseDetailsWithCompDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = courseDetailsWithCompDF.filter(col("competenciesJson").isNotNull)
    df = df.withColumn("competencies", from_json(col("competenciesJson"), courseCompetenciesSchema))

    df = df.select(
      col("courseID"), col("courseName"), col("courseStatus"), col("courseOrgID"),
      explode_outer(col("competencies")).alias("competency")
    )
    df = df.filter(col("competency").isNotNull)
    df = df.withColumn("competencyLevel", expr("TRIM(competency.selectedLevelLevel)"))
    df = df.withColumn("competencyLevel",
      expr("IF(competencyLevel RLIKE '[0-9]+', CAST(REGEXP_EXTRACT(competencyLevel, '[0-9]+', 0) AS INTEGER), 1)"))
    df = df.select(
      col("courseID"), col("courseName"), col("courseStatus"), col("courseOrgID"),
      col("competency.id").alias("competencyID"),
      col("competencyLevel")
    )

    show(df, "courseCompetencyDataFrame (courseID, courseName, courseStatus, courseOrgID, competencyID, competencyLevel)")
    df
  }


  /**
   * data frame of course rating summary
   * @return DataFrame(courseID, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
   */
  def courseRatingSummaryDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
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
   * add course rating columns to course detail data-frame
   * @param courseDetailsDF course details data frame
   * @param courseRatingDF course rating summary data frame
   * @return DataFrame(courseID, courseName, courseStatus, courseDuration, courseResourceCount, courseOrgID, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
   */
  def courseDetailsWithRatingDataFrame(courseDetailsDF: DataFrame, courseRatingDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    // courseDetailsDF = DataFrame(courseID, courseName, courseStatus, courseDuration, courseResourceCount, courseOrgID)
    // courseRatingDF = DataFrame(courseID, ratingSum, ratingCount, ratingAverage, count1Star, count2Star, count3Star, count4Star, count5Star)
    val df = courseDetailsDF.join(courseRatingDF, Seq("courseID"), "left")

    show(df)
    df
  }

  /**
   * get course completion data with details attached
   * @param courseDetailsDF course details data frame
   * @return DataFrame(userID, courseID, courseProgress, courseName, courseStatus, courseDuration, courseResourceCount, courseOrgID, completionPercentage, completionStatus)
   */
  def userCourseCompletionWithDetailsDataFrame(courseDetailsDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    // courseCompletionDF = DataFrame(userID, courseID, courseProgress)
    val courseCompletionDF = cassandraTableAsDataFrame(conf.cassandraCourseKeyspace, conf.cassandraUserEnrolmentsTable)
      .select(
        col("userid").alias("userID"),
        col("courseid").alias("courseID"),
        col("progress").alias("courseProgress")
      ).na.fill(0, Seq("courseProgress"))

    // courseDetailsDF = DataFrame(courseID, courseName, courseStatus, courseDuration, courseResourceCount, courseOrgID)
    var df = courseCompletionDF.join(courseDetailsDF, Seq("courseID"), "inner")
      .withColumn("completionPercentage", expr("CASE WHEN courseProgress=0 THEN 0.0 ELSE 100.0 * courseProgress / courseResourceCount END"))

    df = withCompletionStatusColumn(df)

    show(df)
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them from druid
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel)
   */
  val expectedCompetencySchema: StructType = StructType(Seq(
    StructField("orgID",  StringType, nullable = true),
    StructField("workOrderID",  StringType, nullable = true),
    StructField("userID",  StringType, nullable = true),
    StructField("competencyID",  StringType, nullable = true),
    StructField("expectedCompetencyLevel",  IntegerType, nullable = true)
  ))
  def expectedCompetencyDataFrame()(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
    val query = """SELECT edata_cb_data_deptId AS orgID, edata_cb_data_wa_id AS workOrderID, edata_cb_data_wa_userId AS userID, edata_cb_data_wa_competency_id AS competencyID, CAST(REGEXP_EXTRACT(edata_cb_data_wa_competency_level, '[0-9]+') AS INTEGER) AS expectedCompetencyLevel FROM \"cb-work-order-properties\" WHERE edata_cb_data_wa_competency_type='COMPETENCY' AND edata_cb_data_wa_id IN (SELECT LATEST(edata_cb_data_wa_id, 36) FROM \"cb-work-order-properties\" GROUP BY edata_cb_data_wa_userId)"""
    var df = druidDFOption(query, conf.sparkDruidRouterHost).orNull
    if (df == null) return emptySchemaDataFrame(expectedCompetencySchema)

    df = df.filter(col("competencyID").isNotNull && col("expectedCompetencyLevel").notEqual(0))
      .withColumn("expectedCompetencyLevel", expr("CAST(expectedCompetencyLevel as INTEGER)"))  // Important to cast as integer otherwise a cast will fail later on
      .filter(col("expectedCompetencyLevel").isNotNull && col("expectedCompetencyLevel").notEqual(0))

    show(df)
    df
  }

  /**
   * User's expected competency data from the latest approved work orders issued for them, including live course count
   * @param expectedCompetencyDF expected competency data frame
   * @param courseCompetencyDF course competency data frame
   * @return DataFrame(orgID, workOrderID, userID, competencyID, expectedCompetencyLevel, liveCourseCount)
   */
  def expectedCompetencyWithCourseCountDataFrame(expectedCompetencyDF: DataFrame, courseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
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
    StructField("competencySelfAttestedLevel",  StringType, nullable = true), // this is sometimes an int other times a string
    StructField("competencySelfAttestedLevelValue",  StringType, nullable = true)
  ))
  val profileDetailsSchema: StructType = StructType(Seq(
    StructField("competencies", ArrayType(profileCompetencySchema), nullable = true)
  ))

  val competencyLevelPattern: Regex = ".*[Ll]evel[ ]+?([0-9]+).*".r
  /**
   * match string against level pattern and return level or zero
   * @param s string to parse
   * @return level or zero
   */
  def parseCompetencyLevelString(s: String): Int = {
    s match {
      case competencyLevelPattern(level) => level.toInt
      case _ => 0
    }
  }

  /**
   * get competency level from string value
   * @param levelString level string
   * @return level value as int
   */
  def getCompetencyLevel(levelString: String): Int = {
    intOrZero(levelString) match {
      case 0 => parseCompetencyLevelString(levelString)
      case default => default
    }
  }

  /**
   * spark udf to infer competency level value, returns 1 if no value could be inferred
   * @param csaLevel value of competencySelfAttestedLevel column
   * @param csaLevelValue value of competencySelfAttestedLevelValue column
   * @return level value as int
   */
  def compLevelParser(csaLevel: String, csaLevelValue: String): Int = {
    for (levelString <- Seq(csaLevel, csaLevelValue)) {
      val level = getCompetencyLevel(levelString)
      if (level != 0) return level
    }
    1 // return 1 as default
  }

  /**
   * User's declared competency data from cassandra sunbird:user
   * @return DataFrame(userID, competencyID, declaredCompetencyLevel)
   */
  def declaredCompetencyDataFrame()(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
    val userdata = cassandraTableAsDataFrame(conf.cassandraUserKeyspace, conf.cassandraUserTable)

    // select id and profile details column where profile details are available
    var df = userdata.where(col("profiledetails").isNotNull).select("id", "profiledetails")
    // json parse profile details
    df = df.withColumn("profile", from_json(col("profiledetails"), profileDetailsSchema))
    // explode competencies
    df = df.select(col("id"), explode_outer(col("profile.competencies")).alias("competency"))
    // filter out where competency or competency id not present
    df = df.where(col("competency").isNotNull && col("competency.id").isNotNull)

    // use udf for competency level parsing, as the schema for competency level is broken
    val compLevelParserUdf = udf(compLevelParser _)
    df = df.withColumn("declaredCompetencyLevel",
      compLevelParserUdf(col("competency.competencySelfAttestedLevel"), col("competency.competencySelfAttestedLevelValue"))
    ).na.fill(1, Seq("declaredCompetencyLevel"))  // if competency is listed without a level assume level 1

    // select useful columns
    df = df.select(
      col("id").alias("userID"),
      col("competency.id").alias("competencyID"),
      col("declaredCompetencyLevel")
    )

    show(df, "declaredCompetencyDataFrame [userID, competencyID, declaredCompetencyLevel]")
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api
   * @return DataFrame(competencyID, competencyName, competencyStatus)
   */
  val fracCompetencySchema: StructType = StructType(Seq(
    StructField("competencyID",  StringType, nullable = true),
    StructField("competencyName",  StringType, nullable = true),
    StructField("competencyStatus",  StringType, nullable = true)
  ))
  def fracCompetencyDataFrame()(implicit spark: SparkSession, conf: CMConfig): DataFrame = {
    var df = fracCompetencyDFOption(conf.fracBackendHost).orNull
    if (df == null) return emptySchemaDataFrame(fracCompetencySchema)

    df = df
      .select(explode_outer(col("data.getAllCompetencies")).alias("competency"))
      .select(
        col("competency.id").alias("competencyID"),
        col("competency.name").alias("competencyName"),
        col("competency.status").alias("competencyStatus")
      )
      .where(expr("LOWER(competencyStatus) = 'verified'"))

    show(df)
    df
  }

  /**
   * data frame of all approved competencies from frac dictionary api, including live course count
   * @param fracCompetencyDF frac competency data frame
   * @param courseCompetencyDF course competency data frame
   * @return DataFrame(competencyID, competencyName, competencyStatus, liveCourseCount)
   */
  def fracCompetencyWithCourseCountDataFrame(fracCompetencyDF: DataFrame, courseCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
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
  def fracCompetencyWithOfficerCountDataFrame(fracCompetencyWithCourseCountDF: DataFrame, expectedCompetencyDF: DataFrame, declaredCompetencyDF: DataFrame)(implicit spark: SparkSession, conf: CMConfig) : DataFrame = {
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
   * @param courseCompletionWithDetailsDF user course completion data frame
   * @return DataFrame(userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, completionPercentage, completionStatus)
   */
  def competencyGapCompletionDataFrame(competencyGapDF: DataFrame, courseCompetencyDF: DataFrame, courseCompletionWithDetailsDF: DataFrame): DataFrame = {
    // competencyGapDF - userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap
    // courseCompetencyDF - courseID, courseName, courseStatus, courseOrgID, competencyID, competencyLevel
    // courseCompletionWithDetailsDF - userID, courseID, courseName, courseStatus, courseDuration, courseProgress, courseResourceCount, courseOrgID, completionPercentage, completionStatus

    // userID, competencyID, orgID, workOrderID, expectedCompetencyLevel, declaredCompetencyLevel, competencyGap, courseID, courseName, courseStatus, courseOrgID, competencyLevel
    val cgCourseDF = competencyGapDF.filter("competencyGap > 0")
      .join(courseCompetencyDF, Seq("competencyID"), "leftouter")
      .filter("expectedCompetencyLevel >= competencyLevel")

    // userID, competencyID, orgID, workOrderID, completionPercentage
    val gapCourseUserStatus = cgCourseDF.join(courseCompletionWithDetailsDF, Seq("userID", "courseID"), "leftouter")
      .groupBy("userID", "competencyID", "orgID", "workOrderID")
      .agg(max(col("completionPercentage")).alias("completionPercentage"))
      .withColumn("completionPercentage", expr("IF(ISNULL(completionPercentage), 0.0, completionPercentage)"))

    var df = competencyGapDF.join(gapCourseUserStatus, Seq("userID", "competencyID", "orgID", "workOrderID"), "leftouter")

    df = withCompletionStatusColumn(df)

    show(df)
    df
  }

}