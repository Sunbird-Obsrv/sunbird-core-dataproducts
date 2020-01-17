package org.ekstep.analytics.updater

/**
  * Ref:Design wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794198025/Design+Brainstorm+Data+structure+for+capturing+dashboard+portal+metrics
  * Ref:Implementation wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794099772/Data+Product+Dashboard+summariser+-+Cumulative
  *
  * @author Manjunath Davanam <manjunathd@ilimi.in>
  */

import java.util.Date

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime


case class WorkFlowUsageMetricsAlgoOutput(event_date: Date, total_content_play_sessions: Long, total_timespent: Double, total_interactions: Long, total_pageviews: Long, last_updated_at: Long) extends AlgoOutput with Output


object UpdateWorkFlowUsageMetricsModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, WorkFlowUsageMetricsAlgoOutput, WorkFlowUsageMetricsAlgoOutput] with Serializable {

  val className = "org.ekstep.analytics.updater.UpdateWorkFlowUsageMetricsModel"
  override def name: String = "UpdateWorkFlowUsageMetricsModel"

  /**
    * preProcess which will fetch the `ME_WORKFLOW_USAGE_SUMMARY` Event data from the Cassandra Database.
    *
    * @param data   - RDD Event Data(Empty RDD event)
    * @param config - Configurations to run preProcess
    * @param sc     - SparkContext
    * @return - ME_WORKFLOW_USAGE_SUMMARY
    */
  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DerivedEvent] = {
    DataFilter
      .filter(data, Filter("eid", "EQ", Option("ME_WORKFLOW_USAGE_SUMMARY")))
      .filter(f =>
        "all".equalsIgnoreCase(f.dimensions.did.getOrElse("")) &&
          "all".equalsIgnoreCase(f.dimensions.uid.getOrElse("")) &&
          "all".equalsIgnoreCase(f.dimensions.tag.getOrElse("")) &&
          "all".equalsIgnoreCase(f.dimensions.content_id.getOrElse(""))
      )
  }

  /**
    *
    * @param data   - RDD Workflow summary event data
    * @param config - Configurations to algorithm
    * @param sc     - Spark context
    * @return - DashBoardSummary ->(uniqueDevices, totalContentPlayTime, totalTimeSpent,)
    */
  override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])
                        (implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkFlowUsageMetricsAlgoOutput] = {

    object _constant extends Enumeration {
      val APP = "app"
      val PLAY = "play"
      val CONTENT = "content"
      val SESSION = "session"
      val ALL = "all"
    }
    if (data.count() > 0) {
      val eventDate = data.first().syncts
      val totalContentPlaySessions = data.filter(x =>
        x.dimensions.mode.getOrElse("").equalsIgnoreCase(_constant.PLAY) &&
          x.dimensions.`type`.getOrElse("").equalsIgnoreCase(_constant.CONTENT)).map { f =>
        val eksMap = f.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.getOrElse("total_sessions", 0).asInstanceOf[Number].longValue()
      }.sum().toLong

      val totalTimeSpent = data.filter(x =>
        x.dimensions.`type`.getOrElse("").equalsIgnoreCase(_constant.APP) ||
          x.dimensions.`type`.getOrElse("").equalsIgnoreCase(_constant.SESSION)).map { f =>
        val eksMap = f.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.getOrElse("total_ts", 0.0).asInstanceOf[Number].doubleValue()
      }.sum()

      val totalInteractions = data.filter(x =>
        x.dimensions.`type`.getOrElse("").equalsIgnoreCase(_constant.APP) ||
          x.dimensions.`type`.getOrElse("").equalsIgnoreCase(_constant.SESSION)).map { f =>
        val eksMap = f.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.getOrElse("total_interactions", 0).asInstanceOf[Number].longValue()
      }.sum().toLong

      val totalPageviews = data.filter(x =>
        x.dimensions.`type`.getOrElse("").equalsIgnoreCase(_constant.APP) ||
          x.dimensions.`type`.getOrElse("").equalsIgnoreCase(_constant.SESSION)).map { f =>
        val eksMap = f.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.getOrElse("total_pageviews_count", 0).asInstanceOf[Number].longValue()
      }.sum().toLong

      sc.parallelize(Array(WorkFlowUsageMetricsAlgoOutput(new Date(eventDate), totalContentPlaySessions,
        CommonUtil.roundDouble(totalTimeSpent, 2), totalInteractions, totalPageviews, new DateTime().getMillis)))
    }
    else sc.emptyRDD[WorkFlowUsageMetricsAlgoOutput];
  }

  /**
    *
    * @param data   - RDD DashboardSummary Event
    * @param config - Configurations to run postprocess method
    * @param sc     - Spark context
    * @return - ME_PORTAL_CUMULATIVE_METRICS MeasuredEvents
    */
  override def postProcess(data: RDD[WorkFlowUsageMetricsAlgoOutput], config: Map[String, AnyRef])
                          (implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkFlowUsageMetricsAlgoOutput] = {
    if (data.count() > 0) data.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY)
    data
  }
}