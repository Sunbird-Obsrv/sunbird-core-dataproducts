package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import scala.util.control.Breaks._
import org.ekstep.analytics.dataexhaust._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.util.RequestConfig
import scala.collection.JavaConversions._
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import java.util.Date
import org.joda.time.DateTimeZone
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.Level.INFO
import org.apache.commons.lang3.StringUtils
import org.joda.time.LocalDate

case class DataExhaustOutput(input_events: Long, output_events: Long, dt_first_event: Option[Long] = None, dt_last_event: Option[Long] = None);
case class DataExhaustExeResult(request_id: String, client_key: String, status: Option[String] = Option("PROCESSING"), iteration: Option[Int] = None, err_message: Option[String] = None, input_events: Option[Long] = None, output_events: Option[Long] = None, dt_first_event: Option[Long] = None, dt_last_event: Option[Long] = None, execution_time: Option[Double] = None, dt_job_completed: Option[Long] = None);

object DataExhaustJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.DataExhaustJob"
    implicit val fc = new FrameworkContext();
    def name: String = "DataExhaustJob"
    val rawDataSetList = List("eks-consumption-raw", "eks-creation-raw")

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

        JobLogger.init("DataExhaustJob")
        JobLogger.start("DataExhaust Job Started executing", Option(Map("config" -> config, "model" -> name)))
        val jobConfig = JSONUtils.deserialize[JobConfig](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model));
            try {
                execute(jobConfig);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            execute(jobConfig);
        }
    }

    private def execute(config: JobConfig)(implicit sc: SparkContext) = {

        val requests = DataExhaustUtils.getAllRequest
        if (null != requests) {
            _executeRequests(requests.collect(), config);
        } else {
            JobLogger.end("DataExhaust Job Completed. But There is no job request in DB", "SUCCESS", Option(Map("model" -> name, "date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> 0)));
        }
    }

    private def _executeRequests(requests: Array[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {

        val time = CommonUtil.time({
            val modelParams = config.modelParams.get
            implicit val exhaustConfig = config.exhaustConfig.get
            for (request <- requests) {
                val iteration = request.iteration.getOrElse(0) + 1;
                val requestId = request.request_id;
                val clientKey = request.client_key;
                // JOB_START event
                JobLogger.start("DataExhaust Job Started executing for " + requestId, Option(Map("model" -> name)), "org.ekstep.analytics", requestId)
                println("Processing data-exhaust for request:", requestId);
                try {
                    val requestData = JSONUtils.deserialize[RequestConfig](request.request_data);
                    val eventList = requestData.filter.events.getOrElse(List())
                    val dataSetId = requestData.dataset_id.get
                    val events = if (rawDataSetList.contains(dataSetId) || eventList.size == 0)
                        exhaustConfig.get(dataSetId).get.events
                    else
                        eventList

                    val exeMetrics = CommonUtil.time({
                        for (eventId <- events) yield {
                            _executeEventExhaust(eventId, requestData, request.request_id, request.client_key)
                        }
                    });

                    val inputEventsCount = exeMetrics._2.map { x => x.input_events }.sum;
                    val outputEventsCount = exeMetrics._2.map { x => x.output_events }.sum
                    val firstEventDateList = exeMetrics._2.filter { x => x.dt_first_event.isDefined };
                    val firstEventDate = if (firstEventDateList.size > 0) Option(firstEventDateList.map { x => x.dt_first_event.get }.min) else None;
                    val lastEventDateList = exeMetrics._2.filter { x => x.dt_last_event.isDefined };
                    val lastEventDate = if (lastEventDateList.size > 0) Option(lastEventDateList.map { x => x.dt_last_event.get }.max) else None;
                    val status = if (outputEventsCount > 0) "PENDING_PACKAGING" else "COMPLETED";
                    val completedDate = if (outputEventsCount > 0) None else Option(System.currentTimeMillis());

                    val result = DataExhaustExeResult(requestId, clientKey, Option(status), Option(iteration), None, Option(inputEventsCount), Option(outputEventsCount),
                        firstEventDate, lastEventDate, Option(exeMetrics._1), completedDate);

                    sc.makeRDD(Seq(result)).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "status", "iteration", "err_message", "input_events", "output_events", "dt_first_event", "dt_last_event", "execution_time", "dt_job_completed"));
                    if(status.equals("COMPLETED"))
                        JobLogger.end("DataExhaust Job Completed for "+requestId, "SUCCESS", Option(Map("tag" -> clientKey, "inputEvents" -> inputEventsCount, "outputEvents" -> outputEventsCount, "timeTaken" -> Double.box(exeMetrics._1 / 1000), "channel" -> "in.ekstep")), "org.ekstep.analytics", requestId);

                } catch {
                    case ex: Exception =>
                        ex.printStackTrace()
                        val result = DataExhaustExeResult(requestId, clientKey, Option("FAILED"), Option(iteration), Option(ex.getMessage));
                        sc.makeRDD(Seq(result)).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "status", "iteration", "err_message"));
                }
            }
            if ("true".equals(AppConf.getConfig("data_exhaust.package.enable"))) {
                DataExhaustPackager.execute();
            }
            requests.length
        })
        val requestDetails = DataExhaustUtils.getRequestDetails(new LocalDate().toString)
        val dispatcher = config.output.getOrElse(null)
        if(dispatcher!=null){
            OutputDispatcher.dispatch(dispatcher.head, requestDetails.map { x => JSONUtils.serialize(x) });    
        }
        JobLogger.end("DataExhaust Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> Double.box(time._1 / 1000), "jobCount" -> time._2, "requestDetails" -> requestDetails)));
    }
    private def _executeEventExhaust(eventId: String, request: RequestConfig, requestID: String, clientKey: String)(implicit sc: SparkContext, exhaustConfig: Map[String, DataSet]): DataExhaustOutput = {
        val dataSetID = request.dataset_id.get
        val data = DataExhaustUtils.fetchData(eventId, request, requestID, clientKey)
        val filter = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(request.filter))
        val filteredData = DataExhaustUtils.filterEvent(data, filter, eventId, dataSetID);
        DataExhaustUtils.updateStage(requestID, clientKey, "FILTERED_DATA_" + eventId, "COMPLETED")
        if (!filteredData.isEmpty()) {
            val eventConfig = exhaustConfig.get(dataSetID).get.eventConfig.get(eventId).get
            val outputFormat = request.output_format.getOrElse("json")

            val exhaustRDD = if (eventId.endsWith("-raw") && request.filter.events.isDefined && request.filter.events.get.size > 0) {
                val rdds = for (event <- request.filter.events.get) yield {
                    val filterKey = Filter("eventId", "EQ", Option(event))
                    println("filterKey: "+ JSONUtils.serialize(filterKey))
                    val data = DataFilter.filter(filteredData.map(f => f._2), filterKey).map { x => JSONUtils.serialize(x) }
                    DataExhaustUtils.saveData(data, eventConfig, requestID, event, outputFormat, requestID, clientKey);
                    data;
                }
                sc.union(rdds);
            } else {
                val rdd = filteredData.map { x => JSONUtils.serialize(x._2) };
                DataExhaustUtils.saveData(rdd, eventConfig, requestID, eventId, outputFormat, requestID, clientKey);
                rdd;
            }
            DataExhaustUtils.updateStage(requestID, clientKey, "SAVE_DATA_TO_S3/LOCAL", "COMPLETED");
            if (!exhaustRDD.isEmpty()) {
                val outputRDD = exhaustRDD.map { x => DataExhaustUtils.stringToObject(x, dataSetID) };
                val firstEventDate = outputRDD.sortBy { x => x._1 }.first()._1;
                val lastEventDate = outputRDD.sortBy({ x => x._1 }, false).first._1;
                DataExhaustOutput(data.count, exhaustRDD.count, Option(firstEventDate), Option(lastEventDate));
            } else {
                DataExhaustOutput(data.count, 0);
            }
        } else {
            DataExhaustOutput(data.count, 0);
        }
    }
}