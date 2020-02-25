package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, _}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.sunbird.cloud.storage.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.Level.{ERROR, INFO}


case class DistrictSummary(index:Int, districtName: String, blocks: Long, schools: Long)
case class RootOrgData(rootorgjoinid: String, rootorgchannel: String, rootorgslug: String)

case class SubOrgRow(id: String, isrootorg: Boolean, rootorgid: String, channel: String, status: String, locationid: String, locationids: Seq[String], orgname: String,
                     explodedlocation: String, locid: String, loccode: String, locname: String, locparentid: String, loctype: String, rootorgjoinid: String, rootorgchannel: String, externalid: String)

object StateAdminGeoReportJob extends optional.Application with IJob with StateAdminReportHelper {

  implicit val className: String = "org.ekstep.analytics.job.StateAdminGeoReportJob"

  def name(): String = "StateAdminGeoReportJob"

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
      val blockData = generateGeoReport()
      generateDistrictZip(blockData, config)
      JobLogger.end("StateAdminGeoReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
  }

  def generateGeoReport() (implicit sparkSession: SparkSession, fc: FrameworkContext): DataFrame = {
    
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    
    val organisationDF: DataFrame = loadOrganisationDF()
    val subOrgDF: DataFrame = generateSubOrgData(organisationDF)
    val blockData:DataFrame = generateBlockLevelData(subOrgDF)
    
    blockData.saveToBlobStore(storageConfig, "csv", "geo-detail", Option(Map("header" -> "true")), Option(Seq("slug")))
    
    blockData
          .groupBy(col("slug"))
          .agg(countDistinct("School id").as("schools"),
            countDistinct(col("District id")).as("districts"),
            countDistinct(col("Block id")).as("blocks"))
          .saveToBlobStore(storageConfig, "json", "geo-summary", None, Option(Seq("slug")))

    districtSummaryReport(blockData, storageConfig)
    blockData
  }

  def generateDistrictZip(blockData: DataFrame, jobConfig: JobConfig): Unit = {
    val params = jobConfig.modelParams.getOrElse(Map())
    val virtualEnvDirectory = params.getOrElse("adhoc_scripts_virtualenv_dir", "/mount/venv")
    val scriptOutputDirectory = params.getOrElse("adhoc_scripts_output_dir", "/mount/portal_data")

    val slugs = blockData.select(col("slug")).distinct().collect.map(_.getString(0)).mkString(",")
    val userDetailReportCommand = Seq("bash", "-c",
      s"source $virtualEnvDirectory/bin/activate; " +
      s"dataproducts user_detail --data_store_location='$scriptOutputDirectory' --states='$slugs'")
    JobLogger.log(s"User detail district zip report command:: $userDetailReportCommand", None, INFO)
    val userDetailReportExitCode = ScriptDispatcher.dispatch(userDetailReportCommand)

    if (userDetailReportExitCode == 0) {
      JobLogger.log(s"District level zip generation::Success", None, INFO)
    } else {
      JobLogger.log(s"District level zip generation failed with exit code $userDetailReportExitCode", None, ERROR)
      throw new Exception(s"District level zip generation failed with exit code $userDetailReportExitCode")
    }
  }

  def districtSummaryReport(blockData: DataFrame, storageConfig: StorageConfig)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    val window = Window.partitionBy("slug").orderBy(asc("districtName"))
    val blockDataWithSlug = blockData.
      select("*")
      .groupBy(col("slug"),col("District name").as("districtName")).
      agg(countDistinct("Block id").as("blocks"),countDistinct("externalid").as("schools"))
        .withColumn("index", row_number().over(window))
    blockDataWithSlug.saveToBlobStore(storageConfig, "json", "geo-summary-district", None, Option(Seq("slug")))
    dataFrameToJsonFile(blockDataWithSlug, "geo-summary-district", storageConfig)
  }

  def dataFrameToJsonFile(dataFrame: DataFrame, reportId: String, storageConfig: StorageConfig)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {

    implicit val sc = spark.sparkContext;

    dataFrame.select("slug", "index", "districtName", "blocks", "schools")
      .collect()
      .groupBy(f => f.getString(0)).map(f => {
        val summary = f._2.map(f => DistrictSummary(f.getInt(1), f.getString(2), f.getLong(3), f.getLong(4)))
        val arrDistrictSummary = sc.parallelize(Array(JSONUtils.serialize(summary)), 1)
        OutputDispatcher.dispatch(StorageConfig(storageConfig.store, storageConfig.container, storageConfig.fileName + reportId + "/" + f._1 + ".json", storageConfig.accountKey, storageConfig.secretKey), arrDistrictSummary);
      })

  }
}
