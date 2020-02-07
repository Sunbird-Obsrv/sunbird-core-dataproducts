package org.ekstep.analytics.updater

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, JobLogger, RestUtil}

case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Map[String, AnyRef])

case class ContentMetrics(
                           contentId: String,
                           totalRatingsCount: Long,
                           averageRating: Double,
                           totalTimeSpentInApp: Long,
                           totalTimeSpentInPortal: Long,
                           totalTimeSpentInDeskTop: Long,
                           totalPlaySessionCountInApp: Long,
                           totalPlaySessionCountInPortal: Long,
                           totalPlaySessionCountInDeskTop: Long
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
    //JobLogger.log("content-ids: " + contentList.size + " rating list: " + contentRatingList.size + " filtered list: " + finalList.size, None, Level.INFO)

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
    val response = compute(restUtil.post[List[Map[String, AnyRef]]](apiURL, query))
    //response.map(x=> ContentMetrics(x.averageRating))
    sc.parallelize(response.map(x =>
      ContentMetrics(
        x.getOrElse("ContentId", "").toString,
        x.getOrElse("Number of Ratings", 0.0).asInstanceOf[Number].longValue(),
        x.getOrElse("AverageRating", 0.0).asInstanceOf[Number].doubleValue(),
        x.getOrElse("totalTimeSpentInApp", 0.0).asInstanceOf[Number].longValue(),
        x.getOrElse("totalTimeSpentInPortal", 0.0).asInstanceOf[Number].longValue(),
        x.getOrElse("totalTimeSpentInDeskTop", 0.0).asInstanceOf[Number].longValue(),
        x.getOrElse("totalPlaySessionCountInApp", 0.0).asInstanceOf[Number].longValue(),
        x.getOrElse("totalPlaySessionCountInPortal", 0.0).asInstanceOf[Number].longValue(),
        x.getOrElse("totalPlaySessionCountInDeskTop", 0.0).asInstanceOf[Number].longValue()
      )))
  }

  def compute(response: List[Map[String, AnyRef]]): List[Map[String, AnyRef]] = {
    var listData: List[Map[String, AnyRef]] = List()



    listData
  }

  def publishMetricsToContentModel(contentMetrics: ContentMetrics, baseURL: String, restUtil: HTTPClient): Response = {
    val systemUpdateURL = baseURL + "/" + contentMetrics.contentId
    val request =
      s"""
         |{
         |  "request": {
         |    "content": {
         |      "me_totalRatingsCount": ${contentMetrics.totalRatingsCount},
         |      "me_averageRating": ${contentMetrics.averageRating},
         |      "me_total_time_spent_in_app":${contentMetrics.totalPlaySessionCountInApp},
         |      "me_total_time_spent_in_portal":${contentMetrics.totalTimeSpentInPortal},
         |      "me_total_time_spent_in_desktop":${contentMetrics.totalTimeSpentInDeskTop},
         |      "me_total_plays_session_count_in_app":${contentMetrics.totalPlaySessionCountInApp},
         |      "me_total_play_session_count_in_portal":${contentMetrics.totalPlaySessionCountInDeskTop},
         |      "me_total_play_session_count_in_desktop":${contentMetrics.totalPlaySessionCountInPortal},
         |    }
         |  }
         |}
               """.stripMargin
    val response = restUtil.patch[String](systemUpdateURL, request)
    JSONUtils.deserialize[Response](response)
  }
}