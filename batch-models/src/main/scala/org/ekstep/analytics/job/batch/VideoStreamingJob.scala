package org.ekstep.analytics.job.batch

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{AlgoOutput, FrameworkContext, IJob, JobConfig, JobContext, Level}
import org.ekstep.analytics.util.Constants
import org.ekstep.media.common.{MediaRequest, MediaResponse}
import org.ekstep.media.service.impl.MediaServiceFactory

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import scala.collection.immutable.HashMap

/* Job Request & Data Exhaust */

object VideoStreamingJob extends optional.Application with IJob {

  val formatter: SimpleDateFormat  = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
  val date: Date = new Date();
  case class JobStage(request_id: String, client_key: String, stage: String, stage_status: String, status: String, err_message: String = "", dt_job_processing: Option[java.util.Date] = Option(date))

  case class JobRequest(client_key: String, request_id: String, job_id: Option[String], status: String, request_data: String,
                        location: Option[String], dt_file_created: Option[java.util.Date], dt_first_event: Option[java.util.Date], dt_last_event: Option[java.util.Date],
                        dt_expiration: Option[java.util.Date], iteration: Option[Int], dt_job_submitted: java.util.Date, dt_job_processing: Option[java.util.Date],
                        dt_job_completed: Option[java.util.Date], input_events: Option[Long], output_events: Option[Long], file_size: Option[Long], latency: Option[Int],
                        execution_time: Option[Long], err_message: Option[String], stage: Option[String], stage_status: Option[String], job_name: Option[String] = None) extends AlgoOutput


  // $COVERAGE-OFF$ Disabling scoverage as the below code has no test cases
  implicit val className = "org.ekstep.analytics.job.VideoStreamingJob"
  def name: String = "VideoStreamingJob"
  val mediaService = MediaServiceFactory.getMediaService()
  val contentServiceUrl = AppConf.getConfig("lp.url")

  case class StreamingStage(request_id: String, client_key: String, job_id: String, stage: String, stage_status: String, status: String, iteration: Int, err_message: String = "")

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    JobLogger.init(name)
    JobLogger.start("VideoStreaming Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)

    if (null == sc.getOrElse(null)) {
      JobContext.parallelization = 10
      implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model))
      try {
        execute(jobConfig)
      } finally {
        CommonUtil.closeSparkContext()
      }
    } else {
      implicit val sparkContext: SparkContext = sc.getOrElse(null)
      execute(jobConfig)
    }
  }

  private def execute(config: JobConfig)(implicit sc: SparkContext) = {
      val requests = getAllRequest(10)
      if (null != requests) {
          executeRequests(requests, config)
      } else {
        JobLogger.end("VideoStreaming Job Completed. But There is no job request in DB", "SUCCESS", Option(Map("model" -> name, "date" -> "", "timeTaken" -> 0)))
      }
  }
  
  def getAllRequest(maxIterations: Int)(implicit sc: SparkContext): RDD[JobRequest] = {
      try {
          val jobReq = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).filter { x => x.job_name.getOrElse("").equals("VIDEO_STREAMING") }.filter{ x => x.status.equals("SUBMITTED") || x.status.equals("PROCESSING") || (x.status.equals("FAILED") && x.iteration.getOrElse(0) < maxIterations)}.cache;
          if (!jobReq.isEmpty()) {
            jobReq.map { x => JobStage(x.request_id, x.client_key, "FETCHING_ALL_REQUEST", "COMPLETED", "PROCESSING") }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status", "err_message", "dt_job_processing"))
            jobReq;
          } else {
            null;
          }
      } catch {
          case t: Throwable => {
            t.printStackTrace()
            JobLogger.log("Error while fetching request" + t.getMessage, None, Level.INFO)
            null
          };
      }
  }

  private def executeRequests(requests: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {
      // TODO: add the implementation of Request VideoStreaming
      val submitted = requests.filter(r => List("SUBMITTED", "FAILED").contains(r.status)).cache()
      val processing = requests.filter(r => "PROCESSING".contentEquals(r.status)).cache()

      submitJobRequests(submitted, config)
      getCompletedRequests(processing, config)
      JobLogger.end("VideoStreaming Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> 0, "jobCount" -> 0, "requestDetails" -> null)))
  }

  private def submitJobRequests(submitted: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext): Unit = {
    val stageName = "STREAMING_JOB_SUBMISSION";
    submitted.map { jobRequest =>
      val mediaRequest = MediaRequest(UUID.randomUUID().toString, null, JSONUtils.deserialize[Map[String,AnyRef]](jobRequest.request_data.replaceAll("artifactUrl","artifact_url")))
      val response = mediaService.submitJob(mediaRequest)
      (jobRequest, response)
    }.map {
      x =>
        val request = x._1
        val response = x._2
        val iteration = request.iteration.getOrElse(0)
        if (response.responseCode.equals("OK")) {
            val jobId = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("id","").asInstanceOf[String];
            val jobStatus = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];
            StreamingStage(request.request_id, request.client_key, jobId, stageName, jobStatus, "PROCESSING", iteration);
        } else {
            val errorMsg = JSONUtils.serialize(response.result)
            StreamingStage(request.request_id, request.client_key, null, stageName, "FAILED", "FAILED", iteration + 1, errorMsg);
        }
    }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "job_id", "stage", "stage_status", "status", "iteration", "err_message"))
  }

  private def getCompletedRequests(processing: RDD[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {
    val stageName = "STREAMING_JOB_COMPLETE"
    processing.map { jobRequest =>
      val mediaResponse:MediaResponse = mediaService.getJob(jobRequest.job_id.get)
      (jobRequest, mediaResponse)
    }.map {
      x =>
        val request = x._1
        val response = x._2
        JobLogger.log("Get job details while saving.", Option(response), Level.INFO)
        val iteration = request.iteration.getOrElse(0)
        if(response.responseCode.contentEquals("OK")) {
          val status = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];
          if(status.equalsIgnoreCase("FINISHED")) {
            val streamingUrl = mediaService.getStreamingPaths(request.job_id.get).result.getOrElse("streamUrl","").asInstanceOf[String]
            val requestData = JSONUtils.deserialize[Map[String,AnyRef]](request.request_data).asInstanceOf[Map[String, AnyRef]]
            val contentId = requestData.getOrElse("content_id", "").asInstanceOf[String]
            val channel = requestData.getOrElse("channel", "").asInstanceOf[String]
            if(updatePreviewUrl(contentId, streamingUrl, channel)) {
              val jobStatus = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];
              StreamingStage(request.request_id, request.client_key, request.job_id.get, stageName, jobStatus, "FINISHED", iteration + 1);
            } else {
              null
            }
          } else if(status.equalsIgnoreCase("ERROR")){
            val jobStatus = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];
            StreamingStage(request.request_id, request.client_key, request.job_id.get, stageName, jobStatus, "FAILED", iteration + 1);
          } else {
            null
          }
        } else {
          val errorMsg = JSONUtils.serialize(response.result)
          StreamingStage(request.request_id, request.client_key, null, stageName, "FAILED", "FAILED", iteration + 1, errorMsg);
        }
    }.filter(x =>  x != null).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "job_id", "stage", "stage_status", "status", "iteration", "err_message"))

  }

  private def updatePreviewUrl(contentId: String, streamingUrl: String, channel: String): Boolean = {
    if(!streamingUrl.isEmpty && !contentId.isEmpty) {
      val requestBody = "{\"request\": {\"content\": {\"streamingUrl\":\""+ streamingUrl +"\"}}}"
      val url = contentServiceUrl + "/system/v3/content/update/" + contentId
      val headers = HashMap[String, String]("X-Channel-Id" -> channel)
      val response = RestUtil.patch[Map[String, AnyRef]](url, requestBody, Some(headers))
      if(response.getOrElse("responseCode","").asInstanceOf[String].contentEquals("OK")){
        true;
      } else{
        JobLogger.log("Error while updating previewUrl for content : " + contentId , Option(response), Level.ERROR)
        false
      }
    }else{
      false
    }
  }
  // $COVERAGE-ON$
}