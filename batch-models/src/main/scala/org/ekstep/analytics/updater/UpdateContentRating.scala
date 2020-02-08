package org.ekstep.analytics.updater

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.joda.time.DateTime

case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Map[String, AnyRef])

case class ContentMetrics(
                           contentId: String,
                           totalRatingsCount: Option[Long],
                           averageRating: Option[Double],
                           totalTimeSpentInApp: Option[Long],
                           totalTimeSpentInPortal: Option[Long],
                           totalTimeSpentInDeskTop: Option[Long],
                           totalPlaySessionCountInApp: Option[Long],
                           totalPlaySessionCountInPortal: Option[Long],
                           totalPlaySessionCountInDeskTop: Option[Long]
                         ) extends AlgoOutput with Output

object UpdateContentRating extends IBatchModelTemplate[Empty, Empty, ContentMetrics, ContentMetrics] with Serializable {

  implicit val className = "org.ekstep.analytics.updater.UpdateContentRating"

  override def name: String = "UpdateContentRating"

  override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    data
  }

  override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentMetrics] = {

    val contentList = getRatedContents(config, RestUtil)
    val contentRatingList = getContentMetrics(RestUtil, AppConf.getConfig("druid.content.rating.query"))
    val contentSummaryList = getContentMetrics(RestUtil, AppConf.getConfig("druid.content.summary.query"))

    val finalContentRating = contentRatingList.filter(f => contentList.contains(f.contentId)).map { f =>
      (f.contentId, f)
    }
    val finalContentSummaryList = contentSummaryList.filter(f => contentList.contains(f.contentId)).map { f =>
      (f.contentId, f)
    }
    finalContentRating.join(finalContentSummaryList).map { f =>
      val ratingData = f._2._1
      val consumptionData = f._2._2
      ContentMetrics(f._1,
        ratingData.totalRatingsCount,
        ratingData.averageRating,
        consumptionData.totalTimeSpentInApp,
        consumptionData.totalTimeSpentInPortal,
        consumptionData.totalTimeSpentInDeskTop,
        consumptionData.totalPlaySessionCountInApp,
        consumptionData.totalPlaySessionCountInPortal,
        consumptionData.totalPlaySessionCountInDeskTop
      )
    }
  }

  override def postProcess(data: RDD[ContentMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentMetrics] = {
    val baseURL = AppConf.getConfig("lp.system.update.base.url")
    if (data.count() > 0) {
      data.foreach { f =>
        val response = publishMetricsToContentModel(
          ContentMetrics(
            f.contentId,
            f.totalRatingsCount,
            f.averageRating,
            f.totalTimeSpentInApp,
            f.totalTimeSpentInPortal,
            f.totalTimeSpentInDeskTop,
            f.totalPlaySessionCountInApp,
            f.totalPlaySessionCountInPortal,
            f.totalPlaySessionCountInDeskTop
          ),
          baseURL,
          RestUtil)
        val msg = response.result.getOrElse("messages", List()).asInstanceOf[List[String]].mkString(",")
        JobLogger.log("System Update API request for " + f.contentId + " is " + response.params.status.getOrElse(""), Option(Map("error" -> response.params.errmsg.getOrElse(""), "error_msg" -> msg)), Level.INFO)
      }
    }
    data
  }

  def getRatedContents(config: Map[String, AnyRef], restUtil: HTTPClient): List[String] = {
    val apiURL = AppConf.getConfig("druid.sql.host")
    val startDate = config.getOrElse("startDate", new DateTime().minusDays(1).toString("yyyy-MM-dd")).asInstanceOf[String]
    var endDate = config.getOrElse("endDate", new DateTime().toString("yyyy-MM-dd")).asInstanceOf[String]
    if (startDate.equals(endDate)) endDate = new DateTime(endDate).plusDays(1).toString("yyyy-MM-dd")
    val contentRequest = AppConf.getConfig("druid.unique.content.query").format(new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"))
    val contentResponse = restUtil.post[List[Map[String, AnyRef]]](apiURL, contentRequest)
    contentResponse.map(x => x.getOrElse("Id", "").toString)
  }

  def getContentMetrics(restUtil: HTTPClient, query: String)(implicit sc: SparkContext): RDD[ContentMetrics] = {
    val apiURL = AppConf.getConfig("druid.sql.host")
    sc.parallelize(compute(restUtil.post[List[Map[String, AnyRef]]](apiURL, query)))
  }

  def compute(contentData: List[Map[String, AnyRef]]): List[ContentMetrics] = {
    contentData.map(x => {
      if (x.getOrElse("dimensions_pdata_id", "").toString.contains(".app")) {
        ContentMetrics(
          x.getOrElse("contentId", "").toString, None, None,
          Some(x.getOrElse("total_time_spent", 0).asInstanceOf[Number].longValue()), None, None,
          Some(x.getOrElse("play_sessions_count", 0).asInstanceOf[Number].longValue()), None, None
        )
      }
      if (x.getOrElse("dimensions_pdata_id", "").toString.contains(".portal")) {
        ContentMetrics(
          x.getOrElse("contentId", "").toString, None, None,
          None, Some(x.getOrElse("total_time_spent", 0).asInstanceOf[Number].longValue()), None,
          None, Some(x.getOrElse("play_sessions_count", 0).asInstanceOf[Number].longValue()), None
        )
      }
      if (x.getOrElse("dimensions_pdata_id", "").toString.contains(".desktop")) {
        ContentMetrics(
          x.getOrElse("contentId", "").toString, None, None,
          None, None, Some(x.getOrElse("total_time_spent", 0).asInstanceOf[Number].longValue()),
          None, None, Some(x.getOrElse("play_sessions_count", 0).asInstanceOf[Number].longValue())
        )
      } else {
        ContentMetrics(x.getOrElse("contentId", "").toString, Some(x.getOrElse("totalRatingsCount", 0L).asInstanceOf[Number].longValue()), Some(x.getOrElse("averageRating", 0.0).asInstanceOf[Number].doubleValue()), None, None, None, None, None, None)
      }
    }
    )
  }

  def publishMetricsToContentModel(contentMetrics: ContentMetrics, baseURL: String, restUtil: HTTPClient): Response = {
    val systemUpdateURL = baseURL + "/" + contentMetrics.contentId
    val request =
      s"""
         |{
         |  "request": {
         |    "content": {
         |      "me_totalRatingsCount": ${contentMetrics.totalRatingsCount.orNull},
         |      "me_averageRating": ${contentMetrics.averageRating.orNull},
         |      "me_totalTimeSpent":{
         |      "app": ${contentMetrics.totalPlaySessionCountInApp.orNull},
         |      "portal":${contentMetrics.totalTimeSpentInPortal.orNull},
         |      "desktop":${contentMetrics.totalTimeSpentInDeskTop.orNull}
         |      },
         |      "me_totalPlaySessionCount":{
         |      "app":${contentMetrics.totalPlaySessionCountInApp.orNull},
         |      "portal":${contentMetrics.totalPlaySessionCountInPortal.orNull},
         |      "desktop":${contentMetrics.totalPlaySessionCountInDeskTop.orNull}
         |      }
         |    }
         |  }
         |}
               """.stripMargin

    val response = restUtil.patch[String](systemUpdateURL, JSONUtils.serialize(JSONUtils.deserialize[Map[String, AnyRef]](request)))
    JSONUtils.deserialize[Response](response)
  }
}

