package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, _}
import org.apache.spark.sql.{DataFrame, _}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, _}
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

        generateReport();
        JobLogger.end("StateAdminReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
    }
    
    def generateStateRootSubOrgDF(subOrgDF: DataFrame, claimedShadowDataSummaryDF: DataFrame, claimedShadowUserDF: DataFrame) = {
        val rootSubOrg = subOrgDF.where(col("isrootorg") && col("status").equalTo(1))
        val stateUsersDf = rootSubOrg.join(claimedShadowUserDF, rootSubOrg.col("externalid") === (claimedShadowUserDF.col("orgextid")),"inner")
            .withColumnRenamed("orgname","School name")
            .withColumn("District id", lit("")).withColumn("District name", lit( "")).withColumn("Block id", lit("")).withColumn("Block name", lit(""))
            .select(col("School name"), col("District id"), col("District name"), col("Block id"), col("Block name"), col("slug"),
                col("externalid"),col("userextid"),col("name"),col("userid"))
        stateUsersDf
    }
    
    def generateReport()(implicit sparkSession: SparkSession, fc: FrameworkContext)   = {

        import sparkSession.implicits._

        val shadowDataEncoder = Encoders.product[ShadowUserData].schema
        val shadowUserDF = loadData(sparkSession, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace), Some(shadowDataEncoder)).as[ShadowUserData]
        val claimedShadowUserDF = shadowUserDF.where(col("claimstatus")=== ClaimedStatus.id)
        
        val organisationDF = loadOrganisationDF()
        val channelSlugDF = getChannelSlugDF(organisationDF)
        val shadowDataSummary = generateSummaryData(shadowUserDF)

        val container = AppConf.getConfig("cloud.container.reports")
        val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")
        val storageConfig = getStorageConfig(container, objectKey);
        
        saveUserSummaryReport(shadowDataSummary, channelSlugDF, storageConfig)
        saveUserDetailsReport(shadowUserDF.toDF(), channelSlugDF, storageConfig)

        // Only claimed used
        val claimedShadowDataSummaryDF = claimedShadowUserDF.groupBy("channel")
          .pivot("claimstatus").agg(count("claimstatus")).na.fill(0)

        
        // We can directly write to the slug folder
        val subOrgDF: DataFrame = generateSubOrgData(organisationDF)
        val blockDataWithSlug:DataFrame = generateBlockLevelData(subOrgDF)
        val userDistrictSummaryDF = claimedShadowUserDF.join(blockDataWithSlug, blockDataWithSlug.col("externalid") === (claimedShadowUserDF.col("orgextid")),"inner")
        val validatedUsersWithDst = userDistrictSummaryDF.groupBy(col("slug"), col("Channels")).agg(countDistinct("District name").as("districts"),
            countDistinct("Block id").as("blocks"), countDistinct(claimedShadowUserDF.col("orgextid")).as("schools"), count("userid").as("subOrgRegistered"))
        val validatedShadowDataSummaryDF = claimedShadowDataSummaryDF.join(validatedUsersWithDst, claimedShadowDataSummaryDF.col("channel") === validatedUsersWithDst.col("Channels"))
        val validatedGeoSummaryDF = validatedShadowDataSummaryDF.withColumn("registered",
          when(col("1").isNull, 0).otherwise(col("1"))).withColumn("rootOrgRegistered", col("registered")-col("subOrgRegistered")).drop("1", "channel", "Channels")

        saveUserValidatedSummaryReport(validatedGeoSummaryDF, storageConfig)
        val stateOrgDf = generateStateRootSubOrgDF(subOrgDF, claimedShadowDataSummaryDF, claimedShadowUserDF.toDF());
        saveValidatedUserDetailsReport(userDistrictSummaryDF, storageConfig, "validated-user-detail")
        saveValidatedUserDetailsReport(stateOrgDf, storageConfig, "validated-user-detail-state")
        
        val districtUserResult = userDistrictSummaryDF.groupBy(col("slug"), col("District name").as("districtName")).
            agg(countDistinct("Block id").as("blocks"),countDistinct(claimedShadowUserDF.col("orgextid")).as("schools"), count("userextid").as("registered"))
        saveUserDistrictSummary(districtUserResult, storageConfig)
        
        districtUserResult
    }

    def saveValidatedUserDetailsReport(userDistrictSummaryDF: DataFrame, storageConfig: StorageConfig, reportId: String) : Unit = {
      val window = Window.partitionBy("slug").orderBy(asc("District name"))
      val userDistrictDetailDF = userDistrictSummaryDF.withColumn("Sl", row_number().over(window)).select( col("Sl"), col("District name"), col("District id").as("District ext. ID"),
        col("Block name"), col("Block id").as("Block ext. ID"), col("School name"), col("externalid").as("School ext. ID"), col("name").as("Teacher name"),
        col("userextid").as("Teacher ext. ID"), col("userid").as("Teacher Diksha ID"), col("slug"))
      userDistrictDetailDF.saveToBlobStore(storageConfig, "csv", reportId, Option(Map("header" -> "true")), Option(Seq("slug")))
    }

    def saveUserDistrictSummary(resultDF: DataFrame, storageConfig: StorageConfig)(implicit spark: SparkSession, fc: FrameworkContext) = {
      val window = Window.partitionBy("slug").orderBy(asc("districtName"))
      val districtSummaryDF = resultDF.withColumn("index", row_number().over(window))
      dataFrameToJsonFile(districtSummaryDF, "validated-user-summary-district", storageConfig)
    }

    private def getChannelSlugDF(organisationDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
      organisationDF.filter(col(colName = "slug").isNotNull).select(col("channel"), col("slug")).where(col("isrootorg") && col("status").===(1))
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

        shadowUserDF.transform(transformClaimedStatusValue()).groupBy("channel")
          .pivot("claim_status").agg(count("claim_status")).na.fill(0)
    }

    /**
      * Saves the raw data as a .csv.
      * Appends /detail to the URL to prevent overwrites.
      * Check function definition for the exact column ordering.
      * @param reportDF
      * @param url
      */
    def saveUserDetailsReport(reportDF: DataFrame, channelSlugDF: DataFrame, storageConfig: StorageConfig): Unit = {
        // List of fields available
        //channel,userextid,addedby,claimedon,claimstatus,createdon,email,name,orgextid,phone,processid,updatedon,userid,userids,userstatus

        reportDF.join(channelSlugDF, reportDF.col("channel") === channelSlugDF.col("channel"), "left_outer").select(
              col("slug"),
              col("userextid").as("User external id"),
              col("userstatus").as("User account status"),
              col("userid").as("User id"),
              concat_ws(",", col("userids")).as("Matching User ids"),
              col("claimedon").as("Claimed on"),
              col("orgextid").as("School external id"),
              col("claimstatus").as("Claimed status"),
              col("createdon").as("Created on"),
              col("updatedon").as("Last updated on"))
          .saveToBlobStore(storageConfig, "csv", "user-detail", Option(Map("header" -> "true")), Option(Seq("slug")))
          
        JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    }

    def saveUserValidatedSummaryReport(reportDF: DataFrame, storageConfig: StorageConfig): Unit = {
      reportDF.saveToBlobStore(storageConfig, "json", "validated-user-summary", None, Option(Seq("slug")))
      JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}")
    }

    def saveUserSummaryReport(reportDF: DataFrame, channelSlugDF: DataFrame, storageConfig: StorageConfig): Unit = {
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

        correctedReportDF.join(channelSlugDF, correctedReportDF.col("channel") === channelSlugDF.col("channel"), "left_outer").select(
                col("slug"),
                when(col(UnclaimedStatus.status).isNull, 0).otherwise(col(UnclaimedStatus.status)).as("accounts_unclaimed"),
                when(col(ClaimedStatus.status).isNull, 0).otherwise(col(ClaimedStatus.status)).as("accounts_validated"),
                when(col(RejectedStatus.status).isNull, 0).otherwise(col(RejectedStatus.status)).as("accounts_rejected"),
                when(col(FailedStatus.status).isNull, 0).otherwise(col(FailedStatus.status)).as(FailedStatus.status),
                when(col(MultiMatchStatus.status).isNull, 0).otherwise(col(MultiMatchStatus.status)).as(MultiMatchStatus.status),
                when(col(OrgExtIdMismatch.status).isNull, 0).otherwise(col(OrgExtIdMismatch.status)).as(OrgExtIdMismatch.status))
            .withColumn(
                "accounts_failed",
                col(FailedStatus.status) + col(MultiMatchStatus.status) + col(OrgExtIdMismatch.status))
            .saveToBlobStore(storageConfig, "json", "user-summary", None, Option(Seq("slug")))
        
        JobLogger.log(s"StateAdminReportJob: uploadedSuccess nRecords = ${reportDF.count()}", None, INFO)
    }

  def dataFrameToJsonFile(dataFrame: DataFrame, reportId: String, storageConfig: StorageConfig)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {

    implicit val sc = spark.sparkContext;

    dataFrame.select("slug", "index", "districtName", "blocks", "schools", "registered")
      .collect()
      .groupBy(f => f.getString(0)).map(f => {
        val summary = f._2.map(f => ValidatedUserDistrictSummary(f.getInt(1), f.getString(2), f.getLong(3), f.getLong(4), f.getLong(5)))
        val arrDistrictSummary = sc.parallelize(Array(JSONUtils.serialize(summary)), 1)
        OutputDispatcher.dispatch(StorageConfig(storageConfig.store, storageConfig.container, storageConfig.fileName + reportId + "/" + f._1 + ".json", storageConfig.accountKey, storageConfig.secretKey), arrDistrictSummary);
      })

  }

}
