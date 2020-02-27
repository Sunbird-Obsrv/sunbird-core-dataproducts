package org.ekstep.analytics.job.report

import java.io.File
import java.nio.file.{ Files, StandardCopyOption }
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ col, unix_timestamp, _ }
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{ JSONUtils, JobLogger }
import org.ekstep.analytics.util.ESUtil
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.cloud.storage.conf.AppConf
import org.elasticsearch.spark.sql.sparkDataFrameFunctions
import scala.reflect.ManifestFactory.classType
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.joda.time.DateTimeZone
import org.ekstep.analytics.framework.util.CommonUtil

case class ESIndexResponse(isOldIndexDeleted: Boolean, isIndexLinkedToAlias: Boolean)
case class BatchDetails(batchid: String, courseCompletionCountPerBatch: Long, participantsCountPerBatch: Long)

trait ReportGenerator {
  
  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame
  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, fetchTable: (SparkSession, Map[String, String]) => DataFrame): Unit
}

object CourseMetricsJob extends optional.Application with IJob with ReportGenerator with BaseReportsJob {

  implicit val className = "org.ekstep.analytics.job.CourseMetricsJob"
  val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val sunbirdCoursesKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")

  def name(): String = "CourseMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

    JobLogger.init("CourseMetricsJob")
    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = 10

    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig);
    implicit val frameworkContext = getReportingFrameworkContext();
    execute(jobConfig)
  }

  private def execute(config: JobConfig)(implicit sc: SparkContext, fc: FrameworkContext) = {
    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val renamedDir = s"$tempDir/renamed"
    val sparkConf = sc.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)

    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    prepareReport(spark, storageConfig, loadData)
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))

  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").options(settings).load()
  }

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, loadData: (SparkSession, Map[String, String]) => DataFrame): Unit = {

    val courseBatchDF = loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
    implicit val sparkSession = spark;
    // Get course batches where endDate is null or greater than or equal to yesterday's date

    val timestamp = DateTime.now().minusDays(1).withTimeAtStartOfDay()
    JobLogger.log("Filtering out inactive batches where date is >= " + timestamp, None, INFO)
    val activeBatches = courseBatchDF
      .filter(col("endDate").isNull || unix_timestamp(to_date(col("endDate"), "yyyy-MM-dd")).geq(timestamp.getMillis/1000))
    val newIndex = createESIndex();
    val userData = CommonUtil.time({
      getUserData(loadData)
    });
    Console.println("Time taken to generate user DF", userData._1, "Count", userData._2.count());
    activeBatches.select("batchId", "startDate", "endDate").collect().foreach(f => {
      val batchId = f.getString(0);
      val time1 = CommonUtil.time({
        createReportPerBatch(batchId, f.getString(2), f.getString(1), userData._2, loadData)
      });
      Console.println(s"Time taken to generate $batchId DF", time1._1);
      val reportDF = time1._2;
      val time2 = CommonUtil.time({
        saveReportToBlobStore(batchId, reportDF, storageConfig)
      })
      Console.println(s"Time taken to save report to blobstore for $batchId", time2._1);
      val time3 = CommonUtil.time({
        saveReportToES(batchId, reportDF, newIndex)
      })
      reportDF.unpersist(true);
      Console.println(s"Time taken to save report to ES for $batchId", time3._1);
    })

  }
  
  def getUserData(loadData: (SparkSession, Map[String, String]) => DataFrame)(implicit spark: SparkSession) : DataFrame = {
    
    val userDF = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
    val userOrgDF = loadData(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace)).filter(lower(col("isdeleted")) === "false")
    val organisationDF = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
    val locationDF = loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
    val externalIdentityDF = loadData(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
    
    /**
     * externalIdMapDF - Filter out the external id by idType and provider and Mapping userId and externalId
     */
    val externalIdMapDF = userDF.join(externalIdentityDF, externalIdentityDF.col("idtype") === userDF.col("channel") && externalIdentityDF.col("provider") === userDF.col("channel") && externalIdentityDF.col("userid") === userDF.col("userid"), "inner")
      .select(externalIdentityDF.col("externalid"), externalIdentityDF.col("userid"))

    /*
    * userDenormDF lacks organisation details, here we are mapping each users to get the organisationids
    * */
    val userRootOrgDF = userDF
      .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") === userDF.col("rootorgid"))
      .select(userDF.col("*"), col("organisationid"))

    val userSubOrgDF = userDF
      .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") =!= userDF.col("rootorgid"))
      .select(userDF.col("*"), col("organisationid"))

    val rootOnlyOrgDF = userRootOrgDF
      .join(userSubOrgDF, Seq("userid"), "leftanti")
      .select(userRootOrgDF.col("*"))

    val userOrgDenormDF = rootOnlyOrgDF.union(userSubOrgDF)

    val locationDenormDF = userOrgDenormDF
      .withColumn("exploded_location", explode(col("locationids")))
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
      .dropDuplicates(Seq("userid"))
      .select(col("name").as("district_name"), col("userid"))

    /**
     * Resolve the block name by filtering location type = "BLOCK" for the locationids
     */
    val blockDenormDF = userOrgDenormDF
      .withColumn("exploded_location", explode(col("locationids")))
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "block")
      .dropDuplicates(Seq("userid"))
      .select(col("name").as("block_name"), col("userid"))

    val userLocationResolvedDF = userOrgDenormDF
      .join(locationDenormDF, Seq("userid"), "left_outer")

    val userBlockResolvedDF = userLocationResolvedDF.join(blockDenormDF, Seq("userid"), "left_outer")
    val resolvedExternalIdDF = userBlockResolvedDF.join(externalIdMapDF, Seq("userid"), "left_outer")

    /*
    * Resolve organisation name from `rootorgid`
    * */

    val resolvedOrgNameDF = resolvedExternalIdDF
      .join(organisationDF, organisationDF.col("id") === resolvedExternalIdDF.col("rootorgid"), "left_outer")
      .select(resolvedExternalIdDF.col("userid"), resolvedExternalIdDF.col("rootorgid"), col("orgname").as("orgname_resolved"))
      .dropDuplicates(Seq("userid"))

    /*
    * Resolve school name from `orgid`
    * */
    val schoolNameDF = resolvedExternalIdDF
      .join(organisationDF, organisationDF.col("id") === resolvedExternalIdDF.col("organisationid"), "left_outer")
      .select(resolvedExternalIdDF.col("userid"), resolvedExternalIdDF.col("organisationid"), col("orgname").as("schoolname_resolved"))

    /**
     *  If the user present in the multiple organisation, then zip all the org names.
     *  Example:
     * FROM:
     * +-------+------------------------------------+
     * |userid |orgname                             |
     * +-------+------------------------------------+
     * |user030|SACRED HEART(B)PS,TIRUVARANGAM      |
     * |user030| MPPS BAYYARAM                     |
     * |user001| MPPS BAYYARAM                     |
     * +-------+------------------------------------+
     * TO:
     * +-------+-------------------------------------------------------+
     * |userid |orgname                                                |
     * +-------+-------------------------------------------------------+
     * |user030|[ SACRED HEART(B)PS,TIRUVARANGAM, MPPS BAYYARAM ]      |
     * |user001| MPPS BAYYARAM                                         |
     * +-------+-------------------------------------------------------+
     * Zipping the orgnames of particular userid
     *
     *
     */
    val schoolNameIndexDF = schoolNameDF.withColumn("index", count("userid").over(Window.partitionBy("userid").orderBy("userid")).cast("int"))

    val resolvedSchoolNameDF = schoolNameIndexDF.selectExpr("*").filter(col("index") === 1).drop("organisationid", "index", "batchid")
      .union(schoolNameIndexDF.filter(col("index") =!= 1).groupBy("userid").agg(collect_list("schoolname_resolved").cast("string").as("schoolname_resolved")))
      
      resolvedExternalIdDF
      .join(resolvedSchoolNameDF, Seq("userid"), "left_outer")
      .join(resolvedOrgNameDF, Seq("userid", "rootorgid"), "left_outer")
      .dropDuplicates("userid")
      .cache();
  }

  def createReportPerBatch(batchId: String, endDate: String, startDate: String, userDF: DataFrame, loadData: (SparkSession, Map[String, String]) => DataFrame)(implicit spark: SparkSession): DataFrame = {

    JobLogger.log("Creating report for batch " + batchId, None, INFO)
    val userCoursesDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
    

    /*
    * courseBatchDF has details about the course and batch details for which we have to prepare the report
    * courseBatchDF is the primary source for the report
    * userCourseDF has details about the user details enrolled for a particular course/batch
    * */
    val userCourseDenormDF = userCoursesDF.where(col("batchid") === batchId && lower(userCoursesDF.col("active")).equalTo("true"))
      .withColumn("enddate", lit(endDate))
      .withColumn("startdate", lit(startDate))
      .withColumn(
        "course_completion",
        when(col("completionpercentage").isNull, 0)
          .when(col("completionpercentage") > 100, 100)
          .otherwise(col("completionpercentage")).cast("int"))
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .select(
        col("batchid"),
        col("userid"),
        col("completionpercentage"),
        col("enddate"),
        col("startdate"),
        col("enrolleddate"),
        col("completedon"),
        col("active"),
        col("courseid"),
        col("course_completion"),
        col("generatedOn")
      )
    
    /*
    *userCourseDenormDF lacks some of the user information that need to be part of the report
    *here, it will add some more user details
    * */
    val userDenormDF = userCourseDenormDF
      .join(userDF, Seq("userid"), "inner")
      .select(
        userCourseDenormDF.col("*"),
        col("firstname"),
        col("channel"),
        col("lastname"),
        col("maskedemail"),
        col("maskedphone"),
        col("rootorgid"),
        col("userid"),
        col("locationids"),
        col("externalid"),
        col("orgname_resolved"),
        col("schoolname_resolved"),
        col("district_name"),
        col("block_name"))
        
    /*
    * merge orgName and schoolName based on `userid` and calculate the course progress percentage from `progress` column which is no of content visited/read
    * */
//    val reportDF = resolvedExternalIdDF
//      .join(resolvedSchoolNameDF, Seq("userid"), "left_outer")
//      .join(resolvedOrgNameDF, Seq("userid", "rootorgid"), "left_outer")
//      .dropDuplicates("userid", "batchid").cache()
        
    val reportDF = userDenormDF.cache();
    reportDF.count();
    reportDF;
    
  }

  def createESIndex(): String = {
    val cBatchIndex = AppConf.getConfig("course.metrics.es.index.cbatch")
    val aliasName = AppConf.getConfig("course.metrics.es.alias")
    val newIndexPrefix = AppConf.getConfig("course.metrics.es.index.cbatchstats.prefix")
    val newIndex = suffixDate(newIndexPrefix)
    try {
      val indexList = ESUtil.getIndexName(aliasName)
      val oldIndex = indexList.mkString("")
      if (!oldIndex.equals(newIndex)) ESUtil.rolloverIndex(newIndex, aliasName)
    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR)
        ex.printStackTrace()
      }
    }
    newIndex;
  }

  def saveReportToES(batchId: String, reportDF: DataFrame, newIndex: String)(implicit spark: SparkSession): Unit = {

    import org.elasticsearch.spark.sql._
    val participantsCount = reportDF.count()
    val courseCompletionCount = reportDF.filter(col("course_completion").equalTo(100)).count()

    val batchStatsDF = reportDF
      .select(
        concat_ws(" ", col("firstname"), col("lastname")).as("name"),
        concat_ws(":", col("userid"), col("batchid")).as("id"),
        col("userid").as("userId"),
        col("completedon").as("completedOn"),
        col("maskedemail").as("maskedEmail"),
        col("maskedphone").as("maskedPhone"),
        col("orgname_resolved").as("rootOrgName"),
        col("schoolname_resolved").as("subOrgName"),
        col("startdate").as("startDate"),
        col("enddate").as("endDate"),
        col("courseid").as("courseId"),
        col("generatedOn").as("lastUpdatedOn"),
        col("batchid").as("batchId"),
        col("course_completion").cast("long").as("completedPercent"),
        col("district_name").as("districtName"),
        col("block_name").as("blockName"),
        col("externalid").as("externalId"),
        from_unixtime(unix_timestamp(col("enrolleddate"), "yyyy-MM-dd HH:mm:ss:SSSZ"), "yyyy-MM-dd'T'HH:mm:ss'Z'").as("enrolledOn"))

    import spark.implicits._
    val batchDetails = Seq(BatchDetails(batchId, courseCompletionCount, participantsCount)).toDF

    //.withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    val batchDetailsDF = batchDetails
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .select(
        col("batchid").as("id"),
        col("generatedOn").as("reportUpdatedOn"),
        when(col("courseCompletionCountPerBatch").isNull, 0).otherwise(col("courseCompletionCountPerBatch")).as("completedCount"),
        when(col("participantsCountPerBatch").isNull, 0).otherwise(col("participantsCountPerBatch")).as("participantCount"))

    val cBatchIndex = AppConf.getConfig("course.metrics.es.index.cbatch")
    try {
      batchStatsDF.saveToEs(s"$newIndex/_doc", Map("es.mapping.id" -> "id"))
      JobLogger.log("Indexing batchStatsDF is success for: " + batchId, None, INFO)

      // upsert batch details to cbatch index
      batchDetailsDF.saveToEs(s"$cBatchIndex/_doc", Map("es.mapping.id" -> "id"))

      JobLogger.log(s"CourseMetricsJob: Elasticsearch index stats { $newIndex : { batchId: ${batchId}, totalNoOfRecords: ${batchStatsDF.count()} }}", None, INFO)

    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR)
        ex.printStackTrace()
      }
    }
  }

  def suffixDate(index: String): String = {
    index + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now(DateTimeZone.forID("+05:30")))
  }

  def saveReportToBlobStore(batchId: String, reportDF: DataFrame, storageConfig: StorageConfig): Unit = {
    reportDF
      .select(
        col("externalid").as("External ID"),
        col("userid").as("User ID"),
        concat_ws(" ", col("firstname"), col("lastname")).as("User Name"),
        col("maskedemail").as("Email ID"),
        col("maskedphone").as("Mobile Number"),
        col("orgname_resolved").as("Organisation Name"),
        col("district_name").as("District Name"),
        col("schoolname_resolved").as("School Name"),
        col("block_name").as("Block Name"),
        col("enrolleddate").as("Enrolment Date"),
        concat(col("course_completion").cast("string"), lit("%"))
          .as("Course Progress"),
        col("completedon").as("Completion Date"))
      .saveToBlobStore(storageConfig, "csv", "course-progress-reports/" + "report-" + batchId, Option(Map("header" -> "true")), None)

    val noOfRecords = reportDF.count()
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { batchId: $batchId, totalNoOfRecords: $noOfRecords } ", None, INFO)
  }

}

