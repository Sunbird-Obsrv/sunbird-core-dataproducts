package org.ekstep.analytics.model

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.RestUtil
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import collection.JavaConversions._
import com.flyberrycapital.slack.SlackClient
import net.liftweb.json.Serialization.write
import org.apache.commons.math3.analysis.function.Asin
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher

/**
 * @dataproduct
 * @Summarizer
 *
 * MonitorSummaryModel
 *
 * Functionality
 * 1. Monitor all data products. This would be used to keep track of all data products.
 * Events used - JOB_*
 */
case class SlackMessage(channel: String, username: String, text: String, icon_emoji: String = ":ghost:")
case class JobMonitor(jobs_started: Long, jobs_completed: Long, jobs_failed: Long, total_events_generated: Long, total_ts: Double, syncTs: Long, job_summary: Array[Map[String, Any]], dtange: DtRange) extends AlgoOutput
case class JobSummary(model: String, input_count: Long, output_count: Long, time_taken: Double, status: String, day: Int) extends AlgoOutput
case class ModelMapping(model: String, category: String, input_dependency: String)
case class MonitorMessage(job_name: String, job_id: String, success: Int, number_of_input: Long, number_of_output: Long, time_spent: Double, channel: String, tag: String)

object MonitorSummaryModel extends IBatchModelTemplate[V3Event, V3Event, JobMonitor, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.MonitorSummaryModel"
    
    override def name: String = "MonitorSummaryModel"

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[V3Event] = {
        val filteredData = data.filter { x => (x.eid.equals("JOB_START") || (x.eid.equals("JOB_END"))) }.filter { x => !x.context.pdata.get.pid.get.equals("MonitorSummaryModel") }
        filteredData.sortBy(_.ets)
    }

    override def algorithm(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[JobMonitor] = {
        val jobsStarted = data.filter { x => (x.eid.equals("JOB_START")) }.count()
        val filteresData = data.filter { x => (x.eid.equals("JOB_END")) }
        val edataMap = filteresData.map { x => (x.edata) }
        val jobsCompleted = edataMap.filter { x => (x.status.equals("SUCCESS")) }.count()
        val jobsFailed = edataMap.filter { x => (x.status.equals("FAILED")) }.count()
        val syncTs = data.first().ets
        val totalEventsGenerated = filteresData.map { x => x.edata.data.getOrElse(Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("outputEvents", 0L).asInstanceOf[Number].longValue() }.sum().longValue()
        val totalTs = filteresData.map { x => x.edata.data.getOrElse(Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("timeTaken", 0.0).asInstanceOf[Number].doubleValue() }.sum()

        val jobSummary = filteresData.map { x =>
            val modelId = x.context.pdata.get.id
            val model = x.context.pdata.get.pid.get
            val data = x.edata.data.getOrElse(Map()).asInstanceOf[Map[String, AnyRef]]
            val inputCount = data.getOrElse("inputEvents", 0L).asInstanceOf[Number].longValue()
            val outputCount = data.getOrElse("outputEvents", 0L).asInstanceOf[Number].longValue()
            val timeTaken = data.getOrElse("timeTaken", 0.0).asInstanceOf[Number].floatValue()
            val tag = data.getOrElse("tag", "").asInstanceOf[String]
            val status = x.edata.status
            val errMessage = if(null == x.edata.message) "" else x.edata.status
            val day = CommonUtil.getPeriod(x.ets, DAY)
            Map("model" -> model, "model_id" -> modelId, "input_count" -> inputCount, "output_count" -> outputCount, "time_taken" -> timeTaken, "status" -> status, "day" -> day, "tag" -> tag)
        }.collect()
        sc.parallelize(List(JobMonitor(jobsStarted, jobsCompleted, jobsFailed, totalEventsGenerated, totalTs, syncTs, jobSummary, DtRange(data.first().ets, data.collect().last.ets))))
    }

    override def postProcess(data: RDD[JobMonitor], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[MeasuredEvent] = {

        if(config.getOrElse("pushMetrics", false).asInstanceOf[Boolean]) {
            val jobSumm = data.map(f => f.job_summary).flatMap(f => f)
            val analyticsMetrics = jobSumm.map{f =>
                val jobName = f.get("model").get.asInstanceOf[String]
                val jobId = f.get("model_id").get.asInstanceOf[String]
                val success = if("SUCCESS".equals(f.get("status").get.asInstanceOf[String])) 1 else 0
                val inputCount = f.getOrElse("input_count", 0).asInstanceOf[Long]
                val outputCount = f.getOrElse("output_count", 0).asInstanceOf[Long]
                val timeSpent = f.getOrElse("time_taken", 0.0).asInstanceOf[Float]
                val tag = f.get("tag").get.asInstanceOf[String]
                MonitorMessage(jobName, jobId, success, inputCount, outputCount, timeSpent, "", tag)
            }.map(f => JSONUtils.serialize(f))

            KafkaDispatcher.dispatch(config, analyticsMetrics)
        }

        val messages = messageFormatToSlack(data.first(), config)

        if ("true".equalsIgnoreCase(AppConf.getConfig("monitor.notification.slack"))) {
            for (message <- messages.split("\n\n")) {
                val slackMessage = SlackMessage(AppConf.getConfig("monitor.notification.channel"), AppConf.getConfig("monitor.notification.name"), message);
                try {
                    RestUtil.post[String](AppConf.getConfig("monitor.notification.webhook_url"), JSONUtils.serialize(slackMessage));
                } catch {
                    case e: Exception => println("exception caught:", e.getMessage);
                }
            }
        } else {
            println(messages)
        }
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_MONITOR_SUMMARY", "", "DAY", x.dtange);
            val measures = Map(
                "jobs_start_count" -> x.jobs_started,
                "jobs_completed_count" -> x.jobs_completed,
                "jobs_failed_count" -> x.jobs_failed,
                "total_events_generated" -> x.total_events_generated,
                "total_ts" -> x.total_ts,
                "jobs_summary" -> x.job_summary);

            MeasuredEvent("ME_MONITOR_SUMMARY", System.currentTimeMillis(), x.syncTs, meEventVersion, mid, "", "", None, None,
                Context(PData(config.getOrElse("id", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "MonitorSummarizer").asInstanceOf[String])), None, "DAY", x.dtange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, Option(CommonUtil.getPeriod(x.syncTs, DAY)), None, None, None, None, None, None, None, None, None, None, None, None, None),
                MEEdata(measures), None);
        }
    }

    private def messageFormatToSlack(jobMonitorToSclack: JobMonitor, config: Map[String, AnyRef])(implicit sc: SparkContext): String = {

        val jobsStarted = jobMonitorToSclack.jobs_started
        val jobsCompleted = jobMonitorToSclack.jobs_completed
        val jobsFailed = jobMonitorToSclack.jobs_failed
        val totalEventsGenerated = jobMonitorToSclack.total_events_generated
        val totalTs = secondsToTimeFormat(jobMonitorToSclack.total_ts)
        val jobSummaryCaseClass = jobMonitorToSclack.job_summary.map(f => JobSummary(f.get("model").get.asInstanceOf[String], f.get("input_count").get.asInstanceOf[Number].longValue(), f.get("output_count").get.asInstanceOf[Number].longValue(), f.get("time_taken").get.asInstanceOf[Number].doubleValue(), f.get("status").get.asInstanceOf[String], f.get("day").get.asInstanceOf[Number].intValue()))

        val modelMapping = config.get("model").get.asInstanceOf[List[AnyRef]].map { x => JSONUtils.deserialize[ModelMapping](JSONUtils.serialize(x)) }.toSet
        val consumptionJobSummary = modelStats(jobSummaryCaseClass, modelMapping, "Consumption")
        val creationJobSummary = modelStats(jobSummaryCaseClass, modelMapping, "Creation")
        val recommendationJobSummary = modelStats(jobSummaryCaseClass, modelMapping, "Recommendation")
        val date: String = DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now())
        val title = s"""*Jobs | Monitoring Report | $date*"""
        //total statistics regarding jobs
        val totalStats = s"""Number of Jobs Started: `$jobsStarted`\nNumber of Completed Jobs: `$jobsCompleted` \nNumber of Failed Jobs: `$jobsFailed` \nTotal time taken: `$totalTs`\nTotal events generated: `$totalEventsGenerated`"""
        return title + "\n\n" + totalStats + "\n\n" + consumptionJobSummary + "\n\n" + creationJobSummary + "\n\n" + recommendationJobSummary
    }

    private def warningMessages(modelMapping: Map[String, String], models: Array[JobSummary]): String = {
        var inputEventMap = collection.mutable.Map[String, Long]()
        var outputEventMap = collection.mutable.Map[String, Long]()
        models.map { x =>
            inputEventMap += (x.model -> x.input_count)
            outputEventMap += (x.model -> x.output_count)
        }
        var warnings = ""
        modelMapping.map { x =>
            if (outputEventMap.contains(x._2) && inputEventMap.contains(x._1) && outputEventMap(x._2) != inputEventMap(x._1)) {
                val output = x._2
                val input = x._1
                warnings += s"output of $output NOT EQUALS to input of $input\n"
            }
        }
        warnings
    }

    private def secondsToTimeFormat(seconds: Double): String = {
        val s = (seconds).longValue();
        val m = ((seconds/60) % 60).longValue();
        val h = ((seconds/60/60) % 24).longValue();
        h + ":" + m + ":" + s
    }

    private def jobSummaryMessage(modelName: String, jobsFailed: Int, jobsCompleted: Int, warnings: String, models: String): String = {
        val header = "Model, " + "Input Events, " + "Output Events, " + "Total time(secs), " + "Status, " + "Day"
        if (jobsFailed > 0 && warnings.equals("")) {
            s"""*Number of $modelName Jobs Completed : * `$jobsCompleted` \n*Number of $modelName Jobs Failed: * `$jobsFailed`\n*Detailed Report: *\n$modelName Models:```$header\n$models```\nError: ```Job Failed``` """
        } else if (jobsFailed == 0 && warnings.equals("")) {
            s"""*Number of $modelName Jobs Completed : * `$jobsCompleted` \n*Number of $modelName Jobs Failed: * `$jobsFailed`\n*Detailed Report: *\n$modelName Models:```$header\n$models```\n Status: ```Job Run Completed Successfully```"""
        } else if (jobsFailed == 0 && !warnings.equals("")) {
            s"""*Number of $modelName Jobs Completed : * `$jobsCompleted` \n*Number of $modelName Jobs Failed: * `$jobsFailed`\n*Detailed Report: *\n$modelName Models:```$header\n$models```\nWarnings: ```$warnings```\nStatus: ```Job Run Completed Successfully```"""
        } else {
            s"""*Number of $modelName Jobs Completed : * `$jobsCompleted` \n*Number of $modelName Jobs Failed: * `$jobsFailed`\n*Detailed Report: *\n$modelName Models:```$header\n$models```\nWarnings: ```$warnings```\n Error: ```Job Failed```"""
        }
    }

    private def modelStats(jobSummary: Array[JobSummary], modelMapping: Set[ModelMapping], category: String): String = {
        val modelsCategoryFilter = modelMapping.filter { x => (x.category.equalsIgnoreCase(s"$category")) }
        val modelsSet = modelsCategoryFilter.map { x => x.model }
        val modelMappingWithInputDependency = modelsCategoryFilter.filter { x => !x.input_dependency.equals("None") }.map { x => (x.model, x.input_dependency) }.toMap
        val filterModelsFromJobSummary = jobSummary.filter(item => modelsSet(item.model.trim()))
        val modelsCompleted = filterModelsFromJobSummary.filter { x => x.status.equals("SUCCESS") }.size
        val modelsFailed = filterModelsFromJobSummary.filter { x => x.status.equals("FAILED") }.size
        val modelsWarnings = warningMessages(modelMappingWithInputDependency, filterModelsFromJobSummary)
        val modelsString = filterModelsFromJobSummary.map { x => x.model.trim() + ", " + x.input_count + ", " + x.output_count + ", " + CommonUtil.roundDouble((x.time_taken), 2) + ", " + x.status + ", " + x.day + "\n" }.mkString("")
        jobSummaryMessage(s"$category", modelsFailed, modelsCompleted, modelsWarnings, modelsString)
    }
}