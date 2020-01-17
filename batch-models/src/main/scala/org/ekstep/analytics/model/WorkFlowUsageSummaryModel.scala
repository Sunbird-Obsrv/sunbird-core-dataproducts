package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Period
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.conf.AppConf
import scala.collection.mutable.ListBuffer
import com.datastax.spark.connector._
import org.apache.commons.lang3.StringUtils

/**
 * Case Classes for the data product
 */
case class WorkflowUsageMetricsSummary(wk: WorkflowKey, content_type: String, time_spent: Double, total_sessions: Long, avg_ts_session: Double, interact_events_count: Long, interact_events_per_min: Double, total_pageviews_count: Long, avg_pageviews: Double, dt_range: DtRange, syncts: Long, device_ids: Array[String], unique_users: Array[String], contents: Array[String], pdata: PData) extends AlgoOutput;
case class WorkflowUsageInput(index: WorkflowKey, sessionEvents: Buffer[WorkflowUsageMetricsSummary]) extends Input with AlgoInput
case class WorkflowKey(period: Int, app_id: String, channel: String, `type`: String, mode: String, content_id: String, tag: String, user_id: String, did: String)

object WorkFlowUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, WorkflowUsageInput, WorkflowUsageMetricsSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.WorkFlowUsageSummaryModel"
    override def name: String = "WorkFlowUsageSummaryModel"

    private def getWorkflowUsageSummary(event: DerivedEvent, period: Int, pdata: PData, channel: String, `type`: String, mode: String, contentId: String, tagId: String, did: String, user_id: String, content_type: String): WorkflowUsageMetricsSummary = {

        val wk = WorkflowKey(period, pdata.id, channel, `type`, mode, contentId, tagId, user_id, did);
        val gdata = event.dimensions.gdata;
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val total_ts = eksMap.getOrElse("time_spent", 0.0).asInstanceOf[Number].doubleValue();
        val total_sessions = 1;
        val avg_ts_session = total_ts;
        val total_interactions = eksMap.getOrElse("interact_events_count", 0).asInstanceOf[Number].longValue();
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val impression_summary = eksMap.getOrElse("events_summary", List()).asInstanceOf[List[Map[String, AnyRef]]].filter(p => "IMPRESSION".equals(p.get("id").get))
        val total_pageviews_count = if (impression_summary.size > 0) impression_summary.head.getOrElse("count", 0).asInstanceOf[Number].longValue() else 0;
        val avg_pageviews = total_pageviews_count;
        val content = if(event.`object`.nonEmpty && event.`object`.get.id != null) event.`object`.get.id else ""
        WorkflowUsageMetricsSummary(wk, content_type, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, total_pageviews_count, avg_pageviews, event.context.date_range, event.syncts, Array(event.dimensions.did.getOrElse("")), Array(event.uid), Array(content), pdata);
    }
    
    private def _computeMetrics(events: Buffer[WorkflowUsageMetricsSummary], wk: WorkflowKey): WorkflowUsageMetricsSummary = {

        val firstEvent = events.sortBy { x => x.dt_range.from }.head;
        val lastEvent = events.sortBy { x => x.dt_range.to }.last;
        val date_range = DtRange(firstEvent.dt_range.from, lastEvent.dt_range.to);
        
        val total_ts = CommonUtil.roundDouble(events.map { x => x.time_spent }.sum, 2);
        val total_sessions = events.size
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
        val total_interactions = events.map { x => x.interact_events_count }.sum;
        val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
        val total_pageviews_count = events.map { x => x.total_pageviews_count }.sum;
        val avg_pageviews = if (total_pageviews_count == 0) 0 else CommonUtil.roundDouble((total_pageviews_count / total_sessions), 2)
        val device_ids = if (StringUtils.equals(wk.did, "all")) events.map { x => x.device_ids }.reduce((a, b) => a ++ b).distinct.filterNot(p => p.isEmpty()) else Array("").filterNot(p => p.isEmpty());
        val unique_users = if (StringUtils.equals(wk.user_id, "all")) events.map { x => x.unique_users }.reduce((a, b) => a ++ b).distinct.filterNot(p => ((null == p) || (p.isEmpty))) else Array("").filterNot(p => p.isEmpty());
        val contents = if (StringUtils.equals(wk.content_id, "all")) events.map { x => x.contents }.reduce((a, b) => a ++ b).distinct.filterNot(p => p.isEmpty()) else Array("").filterNot(p => p.isEmpty());
        WorkflowUsageMetricsSummary(wk, events.head.content_type, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, total_pageviews_count, avg_pageviews, date_range, lastEvent.syncts, device_ids, unique_users, contents, firstEvent.pdata);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkflowUsageInput] = {
        val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).filter { x => true == x.active }.map { x => x.tag_id }.collect
        val registeredTags = if (tags.nonEmpty) tags; else Array[String]();

        val sessionEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_WORKFLOW_SUMMARY")));

        val normalizeEvents = sessionEvents.map { event =>

            var list: ListBuffer[WorkflowUsageMetricsSummary] = ListBuffer[WorkflowUsageMetricsSummary]();
            val period = CommonUtil.getPeriod(event.context.date_range.to, Period.DAY);
            // For all
            val pdata = event.dimensions.pdata.get
            val channel = CommonUtil.getChannelId(event)
            val `type` = event.dimensions.`type`.get
            val mode = event.dimensions.mode.getOrElse("")
            val contentId = if(event.`object`.nonEmpty && event.`object`.get.id != null) event.`object`.get.id else ""
            val contentType = if(event.`object`.nonEmpty && event.`object`.get.`type` != null) event.`object`.get.`type` else ""

            val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]

            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", "all", "all", "all", "all");
            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", "all", "all", event.uid, "all");
            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", "all", event.dimensions.did.getOrElse(""), "all", "all");
            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, "all", "all", "all", contentType);

            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", "all", event.dimensions.did.getOrElse(""), event.uid, "all");
            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, "all", "all", event.uid, contentType);
            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, "all", event.dimensions.did.getOrElse(""), "all", contentType);
            list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, "all", event.dimensions.did.getOrElse(""), event.uid, contentType);

            val tags = CommonUtil.getValidTagsForWorkflow(event, registeredTags);
            for (tag <- tags) {
                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", tag, "all", "all", "all");
                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", tag, event.dimensions.did.getOrElse(""), "all", "all");
                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", tag, "all", event.uid, "all");
                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, tag, "all", "all", contentType);

                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, "all", tag, event.dimensions.did.getOrElse(""), event.uid, "all");
                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, tag, event.dimensions.did.getOrElse(""), "all", contentType);
                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, tag, "all", event.uid, contentType);
                list += getWorkflowUsageSummary(event, period, pdata, channel, `type`, mode, contentId, tag, event.dimensions.did.getOrElse(""), event.uid, contentType);

            }
            list.toArray;
        }.flatMap { x => x.map { x => x } };

        normalizeEvents.map { x => (x.wk, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => WorkflowUsageInput(x._1, x._2) };
    }

    override def algorithm(data: RDD[WorkflowUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkflowUsageMetricsSummary] = {

        // Filter out records with empty did/contentId
        val filteredData = data.filter(f => (f.index.did.nonEmpty && f.index.content_id.nonEmpty))
        filteredData.map { x =>
            _computeMetrics(x.sessionEvents, x.index);
        }
    }

    override def postProcess(data: RDD[WorkflowUsageMetricsSummary], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { usageSumm =>
            val mid = CommonUtil.getMessageId("ME_WORKFLOW_USAGE_SUMMARY", usageSumm.wk.user_id + usageSumm.wk.tag + usageSumm.wk.period, "DAY", usageSumm.dt_range, usageSumm.wk.content_id, Option(usageSumm.pdata.id), Option(usageSumm.wk.channel), usageSumm.wk.did);
            val measures = Map(
                "total_ts" -> usageSumm.time_spent,
                "total_sessions" -> usageSumm.total_sessions,
                "avg_ts_session" -> usageSumm.avg_ts_session,
                "total_interactions" -> usageSumm.interact_events_count,
                "avg_interactions_min" -> usageSumm.interact_events_per_min,
                "total_pageviews_count" -> usageSumm.total_pageviews_count,
                "avg_pageviews" -> usageSumm.avg_pageviews,
                "total_users_count" -> usageSumm.unique_users.length,
                "total_content_count" -> usageSumm.contents.length,
                "total_devices_count" -> usageSumm.device_ids.length);
            MeasuredEvent("ME_WORKFLOW_USAGE_SUMMARY", System.currentTimeMillis(), usageSumm.syncts, meEventVersion, mid, "", "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "WorkFlowUsageSummarizer").asInstanceOf[String])), None, "DAY", usageSumm.dt_range),
                Dimensions(Option(usageSumm.wk.user_id), Option(usageSumm.wk.did), None, None, None, None, Option(usageSumm.pdata), None, None, None, Option(usageSumm.wk.tag), Option(usageSumm.wk.period), Option(usageSumm.wk.content_id), None, None, None, None, None, None, None, None, None, None, None, None, None, Option(usageSumm.wk.channel), Option(usageSumm.wk.`type`), Option(usageSumm.wk.mode), Option(usageSumm.content_type)),
                MEEdata(measures), None);
        }
    }
}