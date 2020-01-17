package org.ekstep.analytics.job.report

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.util.ESUtil
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.cloud.storage.conf.AppConf
import scala.collection.{Map, _}
import org.elasticsearch.spark.sql.sparkDataFrameFunctions
import scala.reflect.ManifestFactory.classType

trait ReportGenerator {
  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame

  def prepareReport(spark: SparkSession, fetchTable: (SparkSession, Map[String, String]) => DataFrame): DataFrame

  def saveReportES(reportDF: DataFrame): Unit

  def saveReport(reportDF: DataFrame, url: String): Unit
}

case class ESIndexResponse(isOldIndexDeleted: Boolean, isIndexLinkedToAlias: Boolean)

object CourseMetricsJob extends optional.Application with IJob with ReportGenerator with BaseReportsJob {

  implicit val className = "org.ekstep.analytics.job.CourseMetricsJob"

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

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val reportDF = prepareReport(spark, loadData)
    saveReport(reportDF, tempDir)
    renameReport(tempDir, renamedDir)
    uploadReport(renamedDir)
    saveReportES(reportDF)
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))

  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  def prepareReport(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
    val sunbirdCoursesKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
    val courseBatchDF = loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
    val userCoursesDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
    val userDF = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
    val userOrgDF = loadData(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace)).filter(lower(col("isdeleted")) === "false")
    val organisationDF = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
    val locationDF = loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
    val externalIdentityDF = loadData(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))

    /*
    * courseBatchDF has details about the course and batch details for which we have to prepare the report
    * courseBatchDF is the primary source for the report
    * userCourseDF has details about the user details enrolled for a particular course/batch
    * */
    val userCourseDenormDF = courseBatchDF.join(userCoursesDF, userCoursesDF.col("batchid") === courseBatchDF.col("batchid") && lower(userCoursesDF.col("active")).equalTo("true"), "inner")
      .select(
        userCoursesDF.col("batchid"),
        col("userid"),
        col("completionpercentage"),
        col("enddate"),
        col("startdate"),
        col("enrolleddate"),
        col("completedon"),
        col("active"),
        courseBatchDF.col("courseid"))

    /*
    *userCourseDenormDF lacks some of the user information that need to be part of the report
    *here, it will add some more user details
    * */
    val userDenormDF = userCourseDenormDF
      .join(userDF, Seq("userid"), "inner")
      .select(
        userCourseDenormDF.col("*"),
        col("firstname"),
        col("lastname"),
        col("maskedemail"),
        col("maskedphone"),
        col("rootorgid"),
        col("userid"),
        col("locationids"))
    /**
     * externalIdMapDF - Filter out the external id by idType and provider and Mapping userId and externalId
     */
    val externalIdMapDF = userDF.join(externalIdentityDF, externalIdentityDF.col("idtype") === userDF.col("channel") && externalIdentityDF.col("provider") === userDF.col("channel") && externalIdentityDF.col("userid") === userDF.col("userid"), "inner")
      .select(externalIdentityDF.col("externalid"), externalIdentityDF.col("userid"))

    /*
    * userDenormDF lacks organisation details, here we are mapping each users to get the organisationids
    * */
    val userRootOrgDF = userDenormDF
      .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && userOrgDF.col("organisationid") === userDenormDF.col("rootorgid"))
      .select(userDenormDF.col("*"), col("organisationid"))

    val userSubOrgDF = userDenormDF
      .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && userOrgDF.col("organisationid") =!= userDenormDF.col("rootorgid"))
      .select(userDenormDF.col("*"), col("organisationid"))

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
      .select(resolvedExternalIdDF.col("userid"), resolvedExternalIdDF.col("organisationid"), col("orgname").as("schoolname_resolved"), resolvedExternalIdDF.col("batchid"))

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
    val schoolNameIndexDF = schoolNameDF.withColumn("index", count("userid").over(Window.partitionBy("userid", "batchid").orderBy("userid")).cast("int"))

    val resolvedSchoolNameDF = schoolNameIndexDF.selectExpr("*").filter(col("index") === 1).drop("organisationid", "index", "batchid")
      .union(schoolNameIndexDF.filter(col("index") =!= 1).groupBy("userid").agg(collect_list("schoolname_resolved").cast("string").as("schoolname_resolved")))

    /*
    * merge orgName and schoolName based on `userid` and calculate the course progress percentage from `progress` column which is no of content visited/read
    * */
    resolvedExternalIdDF
      .join(resolvedSchoolNameDF, Seq("userid"), "left_outer")
      .join(resolvedOrgNameDF, Seq("userid", "rootorgid"), "left_outer")
      .withColumn(
        "course_completion",
        when(col("completionpercentage").isNull, 0)
          .when(col("completionpercentage") > 100, 100)
          .otherwise(col("completionpercentage")).cast("int"))
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .dropDuplicates("userid", "batchid")

  }

  def saveReportES(reportDF: DataFrame): Unit = {
    import org.elasticsearch.spark.sql._
    val participantsCountPerBatchDF = reportDF
      .groupBy(col("batchid"))
      .count()
      .withColumn("participantsCountPerBatch", col("count"))
      .drop(col("count"))

    val courseCompletionCountPerBatchDF = reportDF
      .filter(col("course_completion").equalTo(100))
      .groupBy(col("batchid"))
      .count()
      .withColumn("courseCompletionCountPerBatch", col("count"))
      .drop(col("count"))

    val batchStatsDF = participantsCountPerBatchDF
      .join(courseCompletionCountPerBatchDF, Seq("batchid"), "left_outer")
      .join(reportDF, Seq("batchid"))
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

    val batchDetailsDF = participantsCountPerBatchDF
      .join(courseCompletionCountPerBatchDF, Seq("batchid"), "left_outer")
      .join(reportDF, Seq("batchid"))
      .select(
        col("batchid").as("id"),
        col("generatedOn").as("reportUpdatedOn"),
        when(col("courseCompletionCountPerBatch").isNull, 0).otherwise(col("courseCompletionCountPerBatch")).as("completedCount"),
        when(col("participantsCountPerBatch").isNull, 0).otherwise(col("participantsCountPerBatch")).as("participantCount"))
      .distinct()

    val cBatchIndex = AppConf.getConfig("course.metrics.es.index.cbatch")
    val aliasName = AppConf.getConfig("course.metrics.es.alias")
    val newIndexPrefix = AppConf.getConfig("course.metrics.es.index.cbatchstats.prefix")
    val newIndex = suffixDate(newIndexPrefix)
    try {
      val indexList = ESUtil.getIndexName(aliasName)
      val oldIndex = indexList.mkString("")
      batchStatsDF.saveToEs(s"$newIndex/_doc", Map("es.mapping.id" -> "id"))
      JobLogger.log("Indexing batchStatsDF is success: " + newIndex, None, INFO)

      if (!oldIndex.equals(newIndex)) ESUtil.rolloverIndex(newIndex, aliasName)

      // upsert batch details to cbatch index
      batchDetailsDF.saveToEs(s"$cBatchIndex/_doc", Map("es.mapping.id" -> "id"))
      val batchStatsPerBatchCount = batchStatsDF.groupBy("batchId").count().collect().map(_.toSeq)
      val batchStatsCount = batchStatsDF.count()

      val batchDetailsPerBatchCount = batchDetailsDF.groupBy("id").count().collect().map(_.toSeq)
      val batchDetailsCount = batchDetailsDF.count()

      JobLogger.log(s"CourseMetricsJob: Elasticsearch index stats { $newIndex : { perBatchCount: ${JSONUtils.serialize(batchStatsPerBatchCount)}, totalNoOfRecords: $batchStatsCount }, $cBatchIndex: { perBatchCount: ${JSONUtils.serialize(batchDetailsPerBatchCount)}, totalNoOfRecords: $batchDetailsCount } }", None, INFO)

    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR)
        ex.printStackTrace()
      }
    }
  }

  def suffixDate(index: String): String = {
    index + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now())
  }

  def saveReport(reportDF: DataFrame, url: String): Unit = {
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
        col("completedon").as("Completion Date"),
        col("batchid")).coalesce(1)
      .write
      .partitionBy("batchid")
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(url)

    val perBatchCount = reportDF.groupBy("batchid").count().collect().map(_.toSeq)
    val noOfRecords = reportDF.count()
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { perBatchCount: ${JSONUtils.serialize(perBatchCount)}, totalNoOfRecords: $noOfRecords } ", None, INFO)
  }

  def uploadReport(sourcePath: String)(implicit fc: FrameworkContext) = {
    
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageService = getReportStorageService();
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
    storageService.closeContext()
  }

  private def recursiveListFiles(file: File, ext: String): Array[File] = {
    val fileList = file.listFiles
    val extOnly = fileList.filter(file => file.getName.endsWith(ext))
    extOnly ++ fileList.filter(_.isDirectory).flatMap(recursiveListFiles(_, ext))
  }

  private def purgeDirectory(dir: File): Unit = {
    for (file <- dir.listFiles) {
      if (file.isDirectory) purgeDirectory(file)
      file.delete
    }
  }

  def renameReport(tempDir: String, outDir: String) = {
    val regex = """\=.*/""".r // to get batchid from the path "somepath/batchid=12313144/part-0000.csv"
    val temp = new File(tempDir)
    val out = new File(outDir)

    if (!temp.exists()) throw new Exception(s"path $tempDir doesn't exist")

    if (out.exists()) {
      purgeDirectory(out)
      JobLogger.log(s"cleaning out the directory ${out.getPath}")
    } else {
      out.mkdirs()
      JobLogger.log(s"creating the directory ${out.getPath}")
    }

    val fileList = recursiveListFiles(temp, ".csv")

    JobLogger.log(s"moving ${fileList.length} files to ${out.getPath}")

    fileList.foreach(file => {
      val value = regex.findFirstIn(file.getPath).getOrElse("")
      if (value.length > 1) {
        val batchid = value.substring(1, value.length() - 1)
        Files.copy(file.toPath, new File(s"${out.getPath}/report-$batchid.csv").toPath, StandardCopyOption.REPLACE_EXISTING)
      }
    })
  }
}

