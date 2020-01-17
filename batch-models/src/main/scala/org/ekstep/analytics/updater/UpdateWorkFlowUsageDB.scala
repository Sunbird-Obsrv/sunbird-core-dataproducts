package org.ekstep.analytics.updater

import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.{ BloomFilterUtil, Constants, WorkFlowUsageSummaryFact }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.Period._
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import org.ekstep.analytics.util.WorkFlowSummaryIndex

case class WorkFlowUsageSummaryFact_T(d_period: Int, d_channel: String, d_app_id: String, d_tag: String, d_type: String, d_mode: String, d_device_id: String, d_content_id: String, d_user_id: String, m_publish_date: DateTime, m_last_sync_date: DateTime, m_last_gen_date: DateTime, m_total_ts: Double, m_total_sessions: Long, m_avg_ts_session: Double, m_total_interactions: Long, m_avg_interactions_min: Double, m_total_pageviews_count: Long, m_avg_pageviews: Double, m_unique_users: List[String], m_device_ids: List[String], m_contents: List[String], m_content_type: Option[String])

object UpdateWorkFlowUsageDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, WorkFlowUsageSummaryFact, WorkFlowSummaryIndex] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateWorkFlowUsageDB"
    override def name: String = "UpdateWorkFlowUsageDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_WORKFLOW_USAGE_SUMMARY")))
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkFlowUsageSummaryFact] = {
        val workflowSummary = data.map { x =>

            val period = x.dimensions.period.get
            val channel = CommonUtil.getChannelId(x)
            val appId = CommonUtil.getAppDetails(x).id
            val tag = x.dimensions.tag.get
            val summType = x.dimensions.`type`.get
            val did = x.dimensions.did.get
            val contentId = x.dimensions.content_id.get
            val contentType = x.dimensions.content_type
            val uid = x.dimensions.uid.get
            val mode = x.dimensions.mode.getOrElse("")

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]

            val publish_date = new DateTime(x.context.date_range.from)
            val totalTS = eksMap.getOrElse("total_ts", 0.0d).asInstanceOf[Double]
            val totalSess = eksMap.getOrElse("total_sessions", 0).asInstanceOf[Int]
            val avgTSsess = eksMap.getOrElse("avg_ts_session", 0.0d).asInstanceOf[Double]
            val totalIntr = eksMap.getOrElse("total_interactions", 0).asInstanceOf[Int]
            val avgIntrMin = eksMap.getOrElse("avg_interactions_min", 0.0d).asInstanceOf[Double]
            val totalPageviewsCount = eksMap.getOrElse("total_pageviews_count", 0L).asInstanceOf[Number].longValue()
            val avgPageviews = eksMap.getOrElse("avg_pageviews", 0.0d).asInstanceOf[Double]
            val uniqueUsers = List[String]()
            val contents = List[String]()
            val deviceIds = List[String]()
            WorkFlowUsageSummaryFact_T(period, channel, appId, tag, summType, mode, did, contentId, uid,
                publish_date, new DateTime(x.syncts), new DateTime(x.context.date_range.to), totalTS,
                totalSess, avgTSsess, totalIntr, avgIntrMin, totalPageviewsCount, avgPageviews, uniqueUsers,
                deviceIds, contents, contentType)
        }.cache()

        // Roll up summaries
        rollup(workflowSummary, DAY).union(rollup(workflowSummary, WEEK)).union(rollup(workflowSummary, MONTH)).union(rollup(workflowSummary, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[WorkFlowUsageSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkFlowSummaryIndex] = {
        // Update the database
        data.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)
        data.map { x => WorkFlowSummaryIndex(x.d_period, x.d_channel, x.d_app_id, x.d_tag, x.d_type, x.d_mode, x.d_device_id, x.d_content_id, x.d_user_id) };
    }

    /**
     * Rollup daily summaries by period. The period summaries are joined with the previous entries in the database and then reduced to produce new summaries.
     */
    private def rollup(data: RDD[WorkFlowUsageSummaryFact_T], period: Period): RDD[WorkFlowUsageSummaryFact] = {

        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.m_last_gen_date.getMillis, period);
            (WorkFlowSummaryIndex(d_period, x.d_channel, x.d_app_id, x.d_tag, x.d_type, x.d_mode, x.d_device_id, x.d_content_id, x.d_user_id), WorkFlowUsageSummaryFact_T(d_period, x.d_channel, x.d_app_id, x.d_tag, x.d_type, x.d_mode, x.d_device_id, x.d_content_id, x.d_user_id, x.m_publish_date, x.m_last_sync_date, x.m_last_gen_date, x.m_total_ts, x.m_total_sessions, x.m_avg_ts_session, x.m_total_interactions, x.m_avg_interactions_min, x.m_total_pageviews_count, x.m_avg_pageviews, x.m_unique_users, x.m_device_ids, x.m_contents, x.m_content_type));
        }.reduceByKey(reduceWUS)
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT).on(SomeColumns("d_period", "d_channel", "d_app_id", "d_tag", "d_type", "d_mode", "d_device_id", "d_content_id", "d_user_id"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(WorkFlowUsageSummaryFact(index.d_period, index.d_channel, index.d_app_id,
                index.d_tag, index.d_type, index.d_mode, index.d_device_id, index.d_content_id, index.d_user_id,
                newSumm.m_publish_date, newSumm.m_last_sync_date, newSumm.m_last_gen_date, 0.0, 0, 0.0, 0, 0.0, 0,
                0.0, 0, 0, 0, BloomFilterUtil.getDefaultBytes(period), BloomFilterUtil.getDefaultBytes(period),
                BloomFilterUtil.getDefaultBytes(period), newSumm.m_content_type))
            reduce(prvSumm, newSumm, period)
        }
        rollupSummaries
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduce(fact1: WorkFlowUsageSummaryFact, fact2: WorkFlowUsageSummaryFact_T, period: Period): WorkFlowUsageSummaryFact = {
        val publish_date =
            if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date
            else fact1.m_publish_date
        val sync_date =
            if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date
            else fact1.m_last_sync_date

        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2)
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min =
            if (total_interactions == 0 || total_ts == 0) 0d
            else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2)

        val device_bf = BloomFilterUtil.deserialize(period, fact1.m_device_ids)
        val content_bf = BloomFilterUtil.deserialize(period, fact1.m_contents)
        val user_bf = BloomFilterUtil.deserialize(period, fact1.m_unique_users)

        val didCount = BloomFilterUtil.countMissingValues(device_bf, fact2.m_device_ids)
        val device_ids = BloomFilterUtil.serialize(device_bf)

        val contentCount = BloomFilterUtil.countMissingValues(content_bf, fact2.m_contents)
        val content_ids = BloomFilterUtil.serialize(content_bf)

        val usersCount = BloomFilterUtil.countMissingValues(user_bf, fact2.m_unique_users)
        val user_ids = BloomFilterUtil.serialize(user_bf)

        val total_devices_count = didCount + fact1.m_total_devices_count
        val total_users_count = usersCount + fact1.m_total_users_count
        val total_content_count = contentCount + fact1.m_total_content_count
        
        val total_pageviews_count = fact2.m_total_pageviews_count + fact1.m_total_pageviews_count

        val avg_pageviews = CommonUtil.roundDouble((total_pageviews_count / total_sessions), 2)

        WorkFlowUsageSummaryFact(fact1.d_period, fact1.d_channel, fact1.d_app_id, fact1.d_tag, fact1.d_type,
            fact1.d_mode, fact1.d_device_id, fact1.d_content_id, fact1.d_user_id, publish_date, sync_date,
            fact2.m_last_gen_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min,
            total_pageviews_count, avg_pageviews, total_users_count, total_content_count, total_devices_count, user_ids,
            device_ids, content_ids, fact2.m_content_type)
    }

    /**
     * Reducer to rollup two summaries
     */
    private def reduceWUS(fact1: WorkFlowUsageSummaryFact_T, fact2: WorkFlowUsageSummaryFact_T): WorkFlowUsageSummaryFact_T = {

        val publish_date =
            if (fact2.m_publish_date.isBefore(fact1.m_publish_date)) fact2.m_publish_date
            else fact1.m_publish_date
        val sync_date =
            if (fact2.m_last_sync_date.isAfter(fact1.m_last_sync_date)) fact2.m_last_sync_date
            else fact1.m_last_sync_date

        val total_ts = CommonUtil.roundDouble(fact2.m_total_ts + fact1.m_total_ts, 2)
        val total_sessions = fact2.m_total_sessions + fact1.m_total_sessions
        val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
        val total_interactions = fact2.m_total_interactions + fact1.m_total_interactions
        val avg_interactions_min =
            if (total_interactions == 0 || total_ts == 0) 0d
            else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2)
        val total_pageviews_count = fact2.m_total_pageviews_count + fact1.m_total_pageviews_count
        val avg_pageviews = CommonUtil.roundDouble((total_pageviews_count / total_sessions), 2)
        
        val unique_users = (fact2.m_unique_users ++ fact1.m_unique_users).distinct
        val contents = (fact2.m_contents ++ fact1.m_contents).distinct
        val device_ids = (fact2.m_device_ids ++ fact1.m_device_ids).distinct
        
        val total_devices_count = device_ids.length
        val total_users_count = unique_users.length
        val total_content_count = contents.length
        
        WorkFlowUsageSummaryFact_T(fact1.d_period, fact1.d_channel, fact1.d_app_id, fact1.d_tag, fact1.d_type,
            fact1.d_mode, fact1.d_device_id, fact1.d_content_id, fact1.d_user_id, publish_date, sync_date,
            fact2.m_last_gen_date, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min,
            total_pageviews_count, avg_pageviews, unique_users, device_ids, contents, fact1.m_content_type)
    }
}