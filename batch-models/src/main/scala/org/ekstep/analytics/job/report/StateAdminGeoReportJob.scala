package org.ekstep.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, _}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.util.HDFSFileUtils
import org.sunbird.cloud.storage.conf.AppConf


case class DistrictSummary(index:Int, districtName: String, blocks: Long, schools: Long)
case class RootOrgData(rootorgjoinid: String, rootorgchannel: String, rootorgslug: String)

case class SubOrgRow(id: String, isrootorg: Boolean, rootorgid: String, channel: String, status: String, locationid: String, locationids: Seq[String], orgname: String,
                     explodedlocation: String, locid: String, loccode: String, locname: String, locparentid: String, loctype: String, rootorgjoinid: String, rootorgchannel: String, externalid: String)

object StateAdminGeoReportJob extends optional.Application with IJob with StateAdminReportHelper {

  implicit val className: String = "org.ekstep.analytics.job.StateAdminGeoReportJob"
  val fSFileUtils = new HDFSFileUtils(className, JobLogger)

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
      try{
        fSFileUtils.purgeDirectory(renamedDir)
      } catch {
        case t: Throwable => null;
      }
      generateGeoReport()
      uploadReport(renamedDir)
      JobLogger.end("StateAdminGeoReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))


  }

  def generateGeoReport() (implicit sparkSession: SparkSession, fc: FrameworkContext): DataFrame = {
    val organisationDF: DataFrame = loadOrganisationDF()
    val subOrgDF: DataFrame = generateSubOrgData(organisationDF)
    val blockData:DataFrame = generateBlockLevelData(subOrgDF)
    blockData.write
      .partitionBy("slug")
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$detailDir")

    blockData
          .groupBy(col("slug"))
          .agg(countDistinct("School id").as("schools"),
            countDistinct(col("District id")).as("districts"),
            countDistinct(col("Block id")).as("blocks"))
          .coalesce(1)
          .write
          .partitionBy("slug")
          .mode("overwrite")
          .json(s"$summaryDir")

    fSFileUtils.renameReport(summaryDir, renamedDir, ".json", "geo-summary")
    fSFileUtils.renameReport(detailDir, renamedDir, ".csv", "geo-detail")
    fSFileUtils.purgeDirectory(detailDir)
    fSFileUtils.purgeDirectory(summaryDir)
    districtSummaryReport(blockData)
    blockData
  }

  def districtSummaryReport(blockData: DataFrame)(implicit fc: FrameworkContext): Unit = {
    val window = Window.partitionBy("slug").orderBy(asc("districtName"))
    val blockDataWithSlug = blockData.
      select("*")
      .groupBy(col("slug"),col("District name").as("districtName")).
      agg(countDistinct("Block id").as("blocks"),countDistinct("externalid").as("schools"))
        .withColumn("index", row_number().over(window))
    dataFrameToJsonFile(blockDataWithSlug)
  }

  def dataFrameToJsonFile(dataFrame: DataFrame)(implicit fc: FrameworkContext): Unit = {
    val dfMap = dataFrame.select("slug", "index","districtName", "blocks", "schools")
      .collect()
      .groupBy(
        f => f.getString(0)).map(f => {
          val summary = f._2.map(f => DistrictSummary(f.getInt(1), f.getString(2), f.getLong(3), f.getLong(4)))
          val arrDistrictSummary = Array(JSONUtils.serialize(summary))
          val fileName = s"$renamedDir/${f._1}/geo-summary-district.json"
          OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> fileName)), arrDistrictSummary);
      })
  }

  def uploadReport(sourcePath: String)(implicit fc: FrameworkContext) = {
    // Container name can be generic - we dont want to create as many container as many reports
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")

    val storageService = getReportStorageService();
    storageService.upload(container, sourcePath, objectKey, isDirectory = Option(true))
    storageService.closeContext()
  }
}

object StateAdminGeoReportJobTest {

  def main(args: Array[String]): Unit = {
    StateAdminGeoReportJob.main("""{"model":"Test"}""");
  }
}

