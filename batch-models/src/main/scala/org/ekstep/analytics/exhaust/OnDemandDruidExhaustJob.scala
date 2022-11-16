package org.ekstep.analytics.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, when}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext, StorageConfig, _}
import org.ekstep.analytics.model.{OutputConfig, ReportConfig}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.{List, Map}
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.apache.hadoop.conf.Configuration
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.util.BaseDruidQueryProcessor
import org.joda.time.DateTime
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier


case class RequestBody(`type`: String,`params`: Map[String,AnyRef])
case class OnDemandDruidResponse(file: List[String], status: String, statusMsg: String, execTime: Long)
case class Metrics(totalRequests: Option[Int], failedRequests: Option[Int], successRequests: Option[Int])

object OnDemandDruidExhaustJob extends optional.Application with BaseReportsJob with Serializable with IJob with OnDemandBaseExhaustJob with BaseDruidQueryProcessor {
  implicit override val className: String = "org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob"

  val jobId: String = "druid-dataset"
  val jobName: String = "OnDemandDruidExhaustJob"

  def name(): String = "OnDemandDruidExhaustJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    JobLogger.init("OnDemandDruidExhaustJob")
    JobLogger.start("OnDemandDruidExhaustJob Started executing", Option(Map("config" -> config, "model" -> name)))
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val sc: SparkContext = spark.sparkContext
    implicit val sqlContext = new SQLContext(sc)
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)

    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val conf = spark.sparkContext.hadoopConfiguration
    try {
      val res = CommonUtil.time(execute());
      // generate metric event and push it to kafka topic
      val metrics = List(Map("id" -> "total-requests", "value" -> res._2.totalRequests), Map("id" -> "success-requests", "value" -> res._2.successRequests), Map("id" -> "failed-requests", "value" -> res._2.failedRequests), Map("id" -> "time-taken-secs", "value" -> Double.box(res._1 / 1000).asInstanceOf[AnyRef]))
      val metricEvent = getMetricJson("OnDemandDruidExhaustJob", Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS", metrics)
      // $COVERAGE-OFF$
      if (AppConf.getConfig("push.metrics.kafka").toBoolean)
        KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      // $COVERAGE-ON$
      JobLogger.end(s"OnDemandDruidExhaustJob completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "totalRequests" -> res._2.totalRequests, "successRequests" -> res._2.successRequests, "failedRequests" -> res._2.failedRequests)));
    } catch {
      case ex: Exception =>
        // $COVERAGE-OFF$
        JobLogger.log(ex.getMessage, None, ERROR);
        JobLogger.end("OnDemandDruidExhaustJob execution failed", "FAILED",
          Option(Map("model" -> "OnDemandDruidExhaustJob",
            "statusMsg" -> ex.getMessage)));
        // generate metric event and push it to kafka topic in case of failure
        val metricEvent = getMetricJson("OnDemandDruidExhaustJob", Option(new
            DateTime().toString(CommonUtil.dateFormat)), "FAILED", List())
        if (AppConf.getConfig("push.metrics.kafka").toBoolean)
          KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      // $COVERAGE-ON$
    } finally {
      frameworkContext.closeContext();
      spark.close()
      cleanUp()
    }
  }

  def validateRequest(request: JobRequest): Boolean = {
    if (request.request_data != null) true else false
  }

  def markRequestAsProcessing(request: JobRequest): Boolean = {
    request.status = "PROCESSING";
    updateStatus(request);
  }

  def druidPostProcess(data: RDD[DruidOutput], request_id: String, reportConfig: ReportConfig, storageConfig: StorageConfig, sortDfColNames: Option[List[String]])(implicit spark: SparkSession, sqlContext: SQLContext, fc: FrameworkContext, sc: SparkContext, config: JobConfig): OnDemandDruidResponse = {
    val labelsLookup = reportConfig.labels
    val dimFields = reportConfig.metrics.flatMap { m =>
      if (m.druidQuery.dimensions.nonEmpty) m.druidQuery.dimensions.get.map(f => f.aliasName.getOrElse(f.fieldName))
      else if (m.druidQuery.sqlDimensions.nonEmpty) m.druidQuery.sqlDimensions.get.map(f => f.fieldName)
      else List()
    }
    val dataCount = sc.longAccumulator("DruidReportCount")
    val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())
    var fileSavedToBlob = List.empty[String]
    reportConfig.output.foreach { f =>
      var df = getReportDF(RestUtil, JSONUtils.deserialize[OutputConfig](JSONUtils.serialize(f)), data, dataCount).na.fill(0).drop("__time")
      (df.columns).map(f1 => {
        df = df.withColumn(f1, when((col(f1) === "unknown") || (col(f1) === "<NULL>"), "Null").otherwise(col(f1)))
      })
      if (sortDfColNames.isDefined) {
        df = df.sort(sortDfColNames.get.head, sortDfColNames.get.tail: _*)
      }
      df = df.dropDuplicates()
      if (dataCount.value > 0) {
        val metricFields = f.metrics
        val fieldsList = (dimFields ++ metricFields).distinct
        val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
        val filteredDf = df.select(fieldsList.head, fieldsList.tail: _*)
        val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*).na.fill("unknown")
        val reportFinalId = reportConfig.id + "/" + request_id + "_" + reportDate
        fileSavedToBlob = saveReport(renamedDf, JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(config.modelParams.get)) ++
          Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> f.fileParameters, "format" -> f.`type`), None, None)
          .map(f => f._1)
        JobLogger.log(reportConfig.id + "Total Records :" + dataCount.value, None, Level.INFO)
      }
      else {
        JobLogger.log("No data found from druid", None, Level.INFO)
      }
    }
    if (fileSavedToBlob.length > 0) {
      OnDemandDruidResponse(fileSavedToBlob, "SUCCESS", "", System.currentTimeMillis())
    } else {
      OnDemandDruidResponse(List(), "FAILED", "No data found from druid", System.currentTimeMillis())
    }
  }

  def getDate(pattern: String): SimpleDateFormat = {
    new SimpleDateFormat(pattern)
  }

  def processRequest(request: JobRequest, reportConfig: ReportConfig, storageConfig: StorageConfig, sortDfColNames: Option[List[String]])(implicit spark: SparkSession, fc: FrameworkContext, sqlContext: SQLContext, sc: SparkContext, config: JobConfig, conf: Configuration): JobRequest = {
    markRequestAsProcessing(request)
    val requestBody = JSONUtils.deserialize[RequestBody](request.request_data)
    val requestParamsBody = requestBody.`params`
    val reportConfigStr = JSONUtils.serialize(reportConfig)

    var finalConfig = reportConfigStr
    (requestParamsBody.keys).map(ke => {
      if (finalConfig.contains(ke)) {
        finalConfig = finalConfig.replace("$" + ke, requestParamsBody.get(ke).get.toString)
      }
    })

    val finalReportConfig = JSONUtils.deserialize[ReportConfig](finalConfig)
    val druidData: RDD[DruidOutput] = fetchDruidData(finalReportConfig, true, false, false)
    val result = CommonUtil.time(druidPostProcess(druidData, request.request_id, JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(finalReportConfig)), storageConfig, sortDfColNames))
    val response = result._2;
    val failedOnDemandDruidRes = response.status.equals("FAILED")
    if (failedOnDemandDruidRes) {
      markRequestAsFailed(request, response.statusMsg)
    } else {
      val storageConfig = getStorageConfig(config, response.file.head)
      request.download_urls = Option(response.file);
      request.execution_time = Option(result._1);
      processRequestEncryption(storageConfig, request)
      request.status = "SUCCESS";
      request.dt_job_completed = Option(System.currentTimeMillis)
    }
    request
  }

  def updateRequestAsync(request: JobRequest)(implicit conf: Configuration, fc: FrameworkContext): CompletableFuture[JobRequest] = {

    CompletableFuture.supplyAsync(new Supplier[JobRequest]() {
      override def get(): JobRequest = {
        val res = CommonUtil.time(updateRequest(request))
        JobLogger.log("Request is zipped", Some(Map("requestId" -> request.request_id, "timeTakenForZip" -> res._1)), INFO)
        request
      }
    })
  }

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sqlContext: SQLContext, sc: SparkContext, conf: Configuration): Metrics = {
    val requests = getRequests(jobId, None)
    val totalRequests = new AtomicInteger(requests.length)
    val result = for (request <- requests) yield {
      val updRequest: JobRequest = {
        try {
          if (validateRequest(request)) {
            val requestBody = JSONUtils.deserialize[RequestBody](request.request_data)
            val requestType = requestBody.`type`
            // TO:DO
            // Fetch report config from dataset_metadata table
            val datasetConf = getDataSetDetails(requestType)
            val reportConfStr = if(datasetConf.druid_query.nonEmpty) datasetConf.druid_query.get else AppConf.getConfig("druid_query." + requestType)
            val sortDfColNames = JSONUtils.deserialize[Map[String,AnyRef]](reportConfStr).get("sort").asInstanceOf[Option[List[String]]]
            val reportConfig = JSONUtils.deserialize[ReportConfig](reportConfStr)
            val storageConfig = getStorageConfig(config, AppConf.getConfig("collection.exhaust.store.prefix"))
            JobLogger.log("Total Requests are ", Some(Map("jobId" -> jobId, "totalRequests" -> requests.length)), INFO)
            val res = processRequest(request, reportConfig, storageConfig, sortDfColNames)
            JobLogger.log("Request is ",Some(request),INFO)
            JobLogger.log("Report Config is ",Some(reportConfig),INFO)
            JobLogger.log("The Request is processed. Pending zipping", Some(Map("requestId" -> request.request_id, "timeTaken" -> res.execution_time,
              "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
            res
          } else {
            JobLogger.log("Invalid Request", Some(Map("requestId" -> request.request_id, "remainingRequest" -> totalRequests.getAndDecrement())), INFO)
            markRequestAsFailed(request, "Invalid request")
          }
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            markRequestAsFailed(request, "Invalid request")
        }
      }
      updateRequestAsync(updRequest)(spark.sparkContext.hadoopConfiguration, fc)
    }
    CompletableFuture.allOf(result: _*) // Wait for all the async tasks to complete
    val completedResult = result.map(f => f.join()); // Get the completed job requests
    Metrics(totalRequests = Some(requests.length), failedRequests = Some(completedResult.count(x => x.status.toUpperCase() == "FAILED")),
      successRequests = Some(completedResult.count(x => x.status.toUpperCase == "SUCCESS")));
  }
}
