package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Encoders, SQLContext}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, JobDriver, Level}
import org.ekstep.analytics.model.DruidQueryProcessingModel
import org.ekstep.analytics.util.Constants
import org.joda.time
import org.joda.time.DateTime


case class ReportRequest(report_id: String, report_schedule: String, config: String, status: String)
case class ReportStatus(reportId: String, status:String)

object DruidQueryProcessor extends IJob {

  implicit val className = "org.ekstep.analytics.job.DruidQueryProcessor"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    val jobName= "DruidQueryProcessor"
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val modelParams = jobConfig.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    modelParams.getOrElse("mode","standalone") match {
      case "standalone" =>
        JobDriver.run("batch", config, DruidQueryProcessingModel);
      case "batch" =>
        JobLogger.init(jobName)
        JobLogger.start(jobName + " Started executing")
        implicit val sparkContext: SparkContext = sc.getOrElse(CommonUtil.getSparkContext(200, "DruidReports"))
        implicit val frameworkContext: FrameworkContext = fc.getOrElse({
          val storageKey = "azure_storage_key"
          val storageSecret = "azure_storage_secret"
          CommonUtil.getFrameworkContext(Option(Array((AppConf.getConfig("cloud_storage_type"), storageKey, storageSecret))))
        })
        implicit val sqlContext: SQLContext = new SQLContext(sparkContext)
        val result = CommonUtil.time(
          {
            val date = new time.DateTime()
            val reportConfig = getReportConfigs(modelParams.get("batchNumber"), date).collect().map(f => {
              val config = JSONUtils.deserialize[Map[String, AnyRef]](f.config)
              (f.report_id, config)
            })
            val status = reportConfig.map({ case (reportId, config) =>
              try {
                DruidQueryProcessingModel.execute(null,Some(config))
                ReportStatus(reportId,"Success")
              }
              catch {
                case ex: Exception =>
                    JobLogger.log(reportId + " report Failed with error: " + ex.printStackTrace(), None, Level.INFO)
                    ReportStatus(reportId,"Failed")
              }
            })
            status
          }
        )
          val finalStatus = result._2
          val metrics = List(Map("id" -> "total-requests", "value" -> Some(finalStatus.length)),
              Map("id" -> "success-requests", "value" -> Some(finalStatus.count(x => x.status.toUpperCase() == "SUCCESS"))),
              Map("id" -> "failed-requests", "value" -> Some(finalStatus.count(x => x.status.toUpperCase() == "FAILED"))),
              Map("id" -> "time-taken-secs", "value" -> Double.box(result._1 / 1000).asInstanceOf[AnyRef]))
          val metricEvent = getMetricJson("DruidReports", Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS", metrics)
          if (AppConf.getConfig("push.metrics.kafka").toBoolean)
              KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
        JobLogger.end(jobName + " Completed successfully!", "SUCCESS", Option(Map("model" -> jobName,
          "TotalRequests" -> finalStatus.length,
          "SuccessRequests" -> finalStatus.count(x => x.status.toUpperCase() == "SUCCESS")),
          "FailedRequests" -> finalStatus.count(x => x.status.toUpperCase() == "FAILED")))
    }
  }

  def getReportConfigs(batchNumber: Option[AnyRef], date: DateTime)(implicit sqlContext: SQLContext): Dataset[ReportRequest] = {
    val encoder = Encoders.product[ReportRequest]

    val url = String.format("%s%s", AppConf.getConfig("postgres.url"), AppConf.getConfig("postgres.db"))
    val configDf = sqlContext.read.jdbc(url, Constants.DRUID_REPORT_CONFIGS_DEFINITION_TABLE,
      CommonUtil.getPostgresConnectionProps()).as[ReportRequest](encoder)
    val requestsDf = configDf.filter(report => {
      report.report_schedule.toUpperCase() match {
        case "DAILY" => true
        case "WEEKLY" => if (date.dayOfWeek().get() == 1) true else false
        case "MONTHLY" => if (date.dayOfMonth().get() == 1) true else false
        case "ONCE" => true
        case _ => false
      }
    }).filter(f => f.status.toUpperCase.equalsIgnoreCase("ACTIVE"))
    if (batchNumber.isDefined)
      requestsDf.filter(col("batch_number").equalTo(batchNumber.get.asInstanceOf[Int]))
    else
      requestsDf
  }
}