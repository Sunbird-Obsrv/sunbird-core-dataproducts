package org.ekstep.analytics.model

import java.sql.Timestamp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.util.Constants

import scala.collection.mutable.Buffer

case class DeviceIndex(device_id: String, channel: String)
case class DialStats(total_count: Long, success_count: Long, failure_count: Long)
case class DeviceInput(index: DeviceIndex, wfsData: Option[Buffer[DerivedEvent]], rawData: Option[Buffer[V3Event]]) extends AlgoInput
case class DeviceSummary(device_id: String, channel: String, total_ts: Double, total_launches: Long, contents_played: Long, unique_contents_played: Long, content_downloads: Long, dial_stats: DialStats, dt_range: DtRange, syncts: Long, firstAccess: Timestamp) extends AlgoOutput
case class FirstAccessByDeviceID(first_access: Option[Timestamp], device_id: String)

object DeviceSummaryModel extends IBatchModelTemplate[String, DeviceInput, DeviceSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.DeviceSummaryModel"
    override def name: String = "DeviceSummaryModel"

    val db = AppConf.getConfig("postgres.db")
    val url = AppConf.getConfig("postgres.url") + s"$db"
    val connProperties = CommonUtil.getPostgresConnectionProps()

    override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DeviceInput] = {
        val rawEventsList = List("SEARCH", "INTERACT")
        val wfsData = data.filter(f => f.contains("ME_WORKFLOW_SUMMARY")).map(f => JSONUtils.deserialize[DerivedEvent](f)).filter { x => (x.dimensions.did.nonEmpty && StringUtils.isNotBlank(x.dimensions.did.get)) }
        val rawData = data.filter(f => !f.contains("ME_WORKFLOW_SUMMARY")).map(f => JSONUtils.deserialize[V3Event](f)).filter{f => rawEventsList.contains(f.eid) && f.context.did.nonEmpty}.filter { x => (StringUtils.isNotBlank(x.context.did.get) && (StringUtils.equals(x.edata.subtype, "ContentDownload-Success") || x.edata.filters.getOrElse(Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].contains("dialcodes"))) };
        val groupedWfsData = wfsData.map { event =>
            (DeviceIndex(event.dimensions.did.get, event.dimensions.channel.get), Buffer(event))
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        val groupedRawData = rawData.map { event =>
            (DeviceIndex(event.context.did.get, event.context.channel), Buffer(event))
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        groupedWfsData.fullOuterJoin(groupedRawData).map(f => DeviceInput(f._1, f._2._1, f._2._2))
    }

    override def algorithm(data: RDD[DeviceInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DeviceSummary] = {
        val summary = data.map{ f =>
            val index = f.index
            val wfs = f.wfsData.getOrElse(Buffer()).sortBy { x => x.context.date_range.from }
            val raw = f.rawData.getOrElse(Buffer()).sortBy(f => f.ets)
            val startTimestamp = wfs.headOption match {
                case Some(_) => wfs.head.context.date_range.from
                case None => raw.head.ets
            }
            val firstAccess = CommonUtil.getTimestampFromEpoch(startTimestamp)
            val endTimestamp = wfs.headOption match {
                case Some(_) => wfs.last.context.date_range.to
                case None => raw.last.ets
            }
            val syncts = wfs.headOption match {
                case Some(_) => wfs.last.syncts
                case None => CommonUtil.getEventSyncTS(raw.last)
            }
            val (total_ts, total_launches) = wfs.headOption match {
                case Some(_) => (wfs.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_spent").get.asInstanceOf[Double])}.sum, wfs.filter(f => "app".equals(f.dimensions.`type`.getOrElse(""))).length.toLong)
                case None => (0.0, 0L)
            }
            val content_play_events = wfs.filter(f => ("content".equals(f.dimensions.`type`.getOrElse("")) && ("play".equals(f.dimensions.mode.getOrElse("")))))
            val contents_played = content_play_events.length
            val unique_contents_played = if (contents_played > 0) content_play_events.map(f => f.`object`.getOrElse(V3Object("", "", None, None)).id).distinct.filter(f => f.nonEmpty).length else 0L
            val dialcodes_events = raw.filter(f => "SEARCH".equals(f.eid))
            val dial_count = dialcodes_events.length
            val dial_success = dialcodes_events.filter(f => f.edata.size > 0).length
            val dial_failure = dialcodes_events.filter(f => f.edata.size == 0).length
            val content_downloads = raw.filter(f => "INTERACT".equals(f.eid)).length
            (index, DeviceSummary(index.device_id, index.channel, CommonUtil.roundDouble(total_ts, 2), total_launches, contents_played, unique_contents_played, content_downloads, DialStats(dial_count, dial_success, dial_failure), DtRange(startTimestamp, endTimestamp), syncts, firstAccess))
        }

        implicit val sqlContext = new SQLContext(sc)
        val responseDf = sqlContext.sparkSession.read.jdbc(url, Constants.DEVICE_PROFILE_TABLE, connProperties).select("device_id","first_access")
        val encoder = Encoders.product[FirstAccessByDeviceID]
        val firstAccessRDD = responseDf.as[FirstAccessByDeviceID](encoder).rdd

        val postgresData = firstAccessRDD.map(f => {(f.device_id, f.first_access)}).filter(f => f._2.nonEmpty)
        val summaryData = summary.map(f => (f._1.device_id, f._2))
        summaryData.leftOuterJoin(postgresData)
          .map{f =>
              val defaultDate = CommonUtil.getTimestampFromEpoch(0L)
              val firstAccessVal = f._2._2.getOrElse(Option(defaultDate))
              if(firstAccessVal.get != defaultDate) {
                  val first_access = if(firstAccessVal.get.getTime > f._2._1.firstAccess.getTime) f._2._1.firstAccess else firstAccessVal.get
                  f._2._1.copy(firstAccess = first_access)
              }
              else
                  f._2._1
          }
    }

    override def postProcess(data: RDD[DeviceSummary], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[MeasuredEvent] = {
        data.map { x =>

            val mid = CommonUtil.getMessageId("ME_DEVICE_SUMMARY", x.device_id, "DAY", x.dt_range, "NA", None, Option(x.channel))
            val measures = Map(
                "total_ts" -> x.total_ts,
                "total_launches" -> x.total_launches,
                "contents_played" -> x.contents_played,
                "unique_contents_played" -> x.unique_contents_played,
                "content_downloads" -> x.content_downloads,
                "dial_stats" -> x.dial_stats,
                "firstAccess" -> x.firstAccess)
            MeasuredEvent("ME_DEVICE_SUMMARY", System.currentTimeMillis(), x.syncts, "1.0", mid, null, null, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "DeviceSummary").asInstanceOf[String])), None, "DAY", x.dt_range),
                Dimensions(None, Option(x.device_id), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.channel)),
                MEEdata(measures))
        }
    }
}