package org.ekstep.analytics.job.report

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, _}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.{FrameworkContext, _}
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.util.HDFSFileUtils
import org.sunbird.cloud.storage.conf.AppConf

case class ValidatedUserDistrictSummary(index: Int, districtName: String, blocks: Long, schools: Long, registered: Long)
case class UserStatus(id: Long, status: String)
object UnclaimedStatus extends UserStatus(0, "UNCLAIMED")
object ClaimedStatus extends UserStatus(1, "CLAIMED")
object RejectedStatus extends UserStatus(2, "REJECTED")
object FailedStatus extends UserStatus(3, "FAILED")
object MultiMatchStatus extends UserStatus(4, "MULTIMATCH")
object OrgExtIdMismatch extends UserStatus(5, "ORGEXTIDMISMATCH")

case class ShadowUserData(channel: String, userextid: String, addedby: String, claimedon: java.sql.Timestamp, claimstatus: Int,
                          createdon: java.sql.Timestamp, email: String, name: String, orgextid: String, processid: String,
                          phone: String, updatedon: java.sql.Timestamp, userid: String, userids: List[String], userstatus: Int)

// Shadow user summary in the json will have this POJO
case class UserSummary(accounts_validated: Long, accounts_rejected: Long, accounts_unclaimed: Long, accounts_failed: Long)

object StateAdminReportJob extends optional.Application with IJob with StateAdminReportHelper {

    implicit val className: String = "org.ekstep.analytics.job.StateAdminReportJob"

    val fSFileUtils = new HDFSFileUtils(className, JobLogger)

    def name(): String = "StateAdminReportJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

        JobLogger.init(name())
        JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
        val jobConfig = JSONUtils.deserialize[JobConfig](config)
        JobContext.parallelization = 10
        implicit val sparkSession: SparkSession = openSparkSession(jobConfig);
        implicit val frameworkContext = getReportingFrameworkContext();
        execute(jobConfig)
        closeSparkSession()
    }

    private def execute(config: JobConfig)(implicit sparkSession: SparkSession, fc: FrameworkContext) = {

        try{
          fSFileUtils.purgeDirectory(renamedDir)
        } catch {
          case t: Throwable => null;
        }
        generateReport();
        uploadReport(renamedDir)
        JobLogger.end("StateAdminReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
    }

    def generateReport()(implicit sparkSession: SparkSession, fc: FrameworkContext)   = {

        import sparkSession.implicits._

        val shadowDataEncoder = Encoders.product[ShadowUserData].schema
        val shadowUserDF = loadData(sparkSession, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace), Some(shadowDataEncoder)).as[ShadowUserData]
        val claimedShadowUserDF = shadowUserDF.where(col("claimstatus")=== ClaimedStatus.id)

        val shadowDataSummary = generateSummaryData(shadowUserDF)

        saveUserSummaryReport(shadowDataSummary, s"$summaryDir")
        saveUserDetailsReport(shadowUserDF.toDF(), s"$detailDir")

        fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "user-detail")
        fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "user-summary")


        // Purge the directories after copying to the upload staging area
        fSFileUtils.purgeDirectory(detailDir)
        fSFileUtils.purgeDirectory(summaryDir)
        // Only claimed used
        val claimedShadowDataSummaryDF = claimedShadowUserDF.groupBy("channel")
          .pivot("claimstatus").agg(count("claimstatus")).na.fill(0)

        val organisationDF = loadOrganisationDF()
        val channelSlugMap: Map[String, String] = getChannelSlugMap(organisationDF)


        // We can directly write to the slug folder
        val blockDataWithSlug = generateGeoBlockData(organisationDF)
        val userDistrictSummaryDF = blockDataWithSlug.where(col("Block id").isNotNull).join(claimedShadowUserDF, blockDataWithSlug.col("externalid") === (claimedShadowUserDF.col("orgextid")),"left_outer")
        val validatedUsersWithDst = userDistrictSummaryDF.groupBy(col("slug"), col("Channels")).agg(countDistinct("District name").as("districts"),countDistinct("Block id").as("blocks"),countDistinct(claimedShadowUserDF.col("orgextid")).as("schools"))

        val validatedShadowDataSummaryDF = claimedShadowDataSummaryDF.join(validatedUsersWithDst, claimedShadowDataSummaryDF.col("channel") === validatedUsersWithDst.col("Channels"))
        val validatedGeoSummaryDF = validatedShadowDataSummaryDF.withColumn("registered",
          when(col("1").isNull, 0).otherwise(col("1"))).drop("1", "channel", "Channels")

        saveUserValidatedSummaryReport(validatedGeoSummaryDF, s"$summaryDir")
        fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "validated-user-summary")
        fSFileUtils.purgeDirectory(summaryDir)

        saveValidatedUserDetailsReport(userDistrictSummaryDF, s"$detailDir")
        fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "validated-user-detail")
        fSFileUtils.purgeDirectory(detailDir)
        renameChannelDirsToSlug(renamedDir, channelSlugMap)

        val resultDF = userDistrictSummaryDF.groupBy(col("slug"), col("index"), col("District name").as("districtName")).
          agg(countDistinct("Block id").as("blocks"),countDistinct(claimedShadowUserDF.col("orgextid")).as("schools"), count("userextid").as("registered"))
        saveUserDistrictSummary(resultDF)
          resultDF
    }

    def saveValidatedUserDetailsReport(userDistrictSummaryDF: DataFrame, url: String) {
      val window = Window.partitionBy("slug").orderBy(asc("District name"))
      val userDistrictDetailDF = userDistrictSummaryDF.withColumn("Sl", row_number().over(window)).select( col("Sl"), col("District name"), col("District id").as("District ext. ID"),
        col("Block name"), col("Block id").as("Block ext. ID"), col("School name"), col("externalid").as("School ext. ID"), col("name").as("Teacher name"),
        col("userextid").as("Teacher ext. ID"), col("userid").as("Teacher Diksha ID"), col("slug"))
      userDistrictDetailDF
        .coalesce(1)
        .write
        .partitionBy("slug")
        .mode("overwrite")
        .option("header", "true")
        .csv(url)
    }

    def saveUserDistrictSummary(resultDF: DataFrame)(implicit fc: FrameworkContext) {
      val window = Window.partitionBy("slug").orderBy(asc("districtName"))
      val districtSummaryDF = resultDF.withColumn("index", row_number().over(window))
      dataFrameToJsonFile(districtSummaryDF)
    }

    private def getChannelSlugMap(organisationDF: DataFrame)(implicit sparkSession: SparkSession): Map[String, String] = {
      val channelSlugMap: Map[String, String] = organisationDF
        .select(col("channel"), col("slug")).where(col("isrootorg") && col("status").===(1))
        .collect().groupBy(f => f.get(0).asInstanceOf[String]).mapValues(f => f.head.get(1).asInstanceOf[String]);
      return channelSlugMap
    }

    def generateSummaryData(shadowUserDF: Dataset[ShadowUserData])(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        def transformClaimedStatusValue()(ds: Dataset[ShadowUserData]) = {
            ds.withColumn(
                "claim_status",
                when($"claimstatus" === UnclaimedStatus.id, lit(UnclaimedStatus.status))
                  .when($"claimstatus" === ClaimedStatus.id, lit(ClaimedStatus.status))
                  .when($"claimstatus" === FailedStatus.id, lit(FailedStatus.status))
                  .when($"claimstatus" === RejectedStatus.id, lit(RejectedStatus.status))
                  .when($"claimstatus" === MultiMatchStatus.id, lit(MultiMatchStatus.status))
                  .when($"claimstatus" === OrgExtIdMismatch.id, lit(OrgExtIdMismatch.status))
                  .otherwise(lit("")))
        }

        val shadowDataSummary = shadowUserDF.transform(transformClaimedStatusValue()).groupBy("channel")
          .pivot("claim_status").agg(count("claim_status")).na.fill(0)

        shadowDataSummary
    }

    /**
      * Saves the raw data as a .csv.
      * Appends /detail to the URL to prevent overwrites.
      * Check function definition for the exact column ordering.
      * @param reportDF
      * @param url
      */
    def saveUserDetailsReport(reportDF: DataFrame, url: String): Unit = {
        // List of fields available
        //channel,userextid,addedby,claimedon,claimstatus,createdon,email,name,orgextid,phone,processid,updatedon,userid,userids,userstatus

        reportDF.coalesce(1)
          .select(
              col("channel"),
              col("userextid").as("User external id"),
              col("userstatus").as("User account status"),
              col("userid").as("User id"),
              concat_ws(",", col("userids")).as("Matching User ids"),
              col("claimedon").as("Claimed on"),
              col("orgextid").as("School external id"),
              col("claimstatus").as("Claimed status"),
              col("createdon").as("Created on"),
              col("updatedon").as("Last updated on"))
          .write
          .partitionBy("channel")
          .mode("overwrite")
          .option("header", "true")
          .csv(url)

        JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    }

    def saveUserValidatedSummaryReport(reportDF: DataFrame, url: String): Unit = {
      reportDF
        .coalesce(1)
        .write
        .partitionBy("slug")
        .mode("overwrite")
        .json(url)

      JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    }

    def saveUserSummaryReport(reportDF: DataFrame, url: String): Unit = {
        val dfColumns = reportDF.columns.toSet

        // Get claim status not in the current dataframe to add them.
        val columns: Seq[String] = Seq(
            UnclaimedStatus.status,
            ClaimedStatus.status,
            RejectedStatus.status,
            FailedStatus.status,
            MultiMatchStatus.status,
            OrgExtIdMismatch.status).filterNot(dfColumns)
        val correctedReportDF = columns.foldLeft(reportDF)((acc, col) => {
            acc.withColumn(col, lit(0))
        })
        JobLogger.log(s"columns to add in this report $columns")

        correctedReportDF.coalesce(1)
          .select(
              col("channel"),
              when(col(UnclaimedStatus.status).isNull, 0).otherwise(col(UnclaimedStatus.status)).as("accounts_unclaimed"),
              when(col(ClaimedStatus.status).isNull, 0).otherwise(col(ClaimedStatus.status)).as("accounts_validated"),
              when(col(RejectedStatus.status).isNull, 0).otherwise(col(RejectedStatus.status)).as("accounts_rejected"),
              when(col(FailedStatus.status).isNull, 0).otherwise(col(FailedStatus.status)).as(FailedStatus.status),
              when(col(MultiMatchStatus.status).isNull, 0).otherwise(col(MultiMatchStatus.status)).as(MultiMatchStatus.status),
              when(col(OrgExtIdMismatch.status).isNull, 0).otherwise(col(OrgExtIdMismatch.status)).as(OrgExtIdMismatch.status))
          .withColumn(
              "accounts_failed",
              col(FailedStatus.status) + col(MultiMatchStatus.status) + col(OrgExtIdMismatch.status))
          .write
          .partitionBy("channel")
          .mode("overwrite")
          .json(url)

        JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}", None, INFO)
    }

    private def renameChannelDirsToSlug(sourcePath: String, channelSlugMap: Map[String, String]) = {
        val files = fSFileUtils.getSubdirectories(sourcePath)
        files.map { oneChannelDir =>
            val channelName = oneChannelDir.getName()
            val slugName = channelSlugMap.get(channelName);
            if(slugName.nonEmpty && !slugName.mkString.equals(channelName)) {
                println(s"channelName = ${channelName} and slugname = ${slugName}")
                val newDirName = oneChannelDir.getParent() + "/" + slugName.get.asInstanceOf[String]
                if(new File(newDirName).exists) {
                  val jsonFiles = fSFileUtils.recursiveListFiles(oneChannelDir, ".json")
                  fSFileUtils.copyFilesToDir(jsonFiles, newDirName)
                  val csvFiles = fSFileUtils.recursiveListFiles(oneChannelDir, ".csv")
                  fSFileUtils.copyFilesToDir(csvFiles, newDirName)
                  fSFileUtils.purgeDirectory(oneChannelDir.getAbsolutePath())
                } else {
                  fSFileUtils.renameDirectory(oneChannelDir.getAbsolutePath(), newDirName)
                }
            } else {
                println("Slug not found or already exists for - " + channelName);
            }
        }
    }

    def uploadReport(sourcePath: String)(implicit fc: FrameworkContext) = {
        // Container name can be generic - we dont want to create as many container as many reports
        val container = AppConf.getConfig("cloud.container.reports")
        val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

        val storageService = getReportStorageService();
        storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
        storageService.closeContext()
    }

    def dataFrameToJsonFile(dataFrame: DataFrame)(implicit fc: FrameworkContext): Unit = {
      val dfMap = dataFrame.select("slug", "index","districtName", "blocks", "schools","registered")
        .collect()
        .groupBy(
          f => f.getString(0)).map(f => {
        val summary = f._2.map(f => ValidatedUserDistrictSummary(f.getInt(1), f.getString(2), f.getLong(3), f.getLong(4), f.getLong(5)))
        val arrDistrictSummary = Array(JSONUtils.serialize(summary))
        val fileName = s"$renamedDir/${f._1}/validated-user-summary-district.json"
        OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> fileName)), arrDistrictSummary);
      })

    }

}

object StateAdminReportJobTest {

  def main(args: Array[String]): Unit = {
    StateAdminReportJob.main("""{"model":"Test"}""");
  }
}