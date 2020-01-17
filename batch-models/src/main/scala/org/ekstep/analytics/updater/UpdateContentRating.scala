package org.ekstep.analytics.updater

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, JobLogger, RestUtil}

case class ContentRatingResult(contentId: String, numOfTimesRated: Long, averageRating: Double) extends AlgoOutput with Output
case class Response(id: String, ver: String, ts: String, params: Params, responseCode: String, result: Map[String, AnyRef])

object UpdateContentRating  extends IBatchModelTemplate[Empty, Empty, ContentRatingResult, ContentRatingResult] with Serializable {

  implicit val className = "org.ekstep.analytics.updater.UpdateContentRating"
  override def name: String = "UpdateContentRating"

  override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
      data
  }

  override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentRatingResult] = {

      val contentList = getRatedContents(config, RestUtil)
      val contentRatingList = getContentRatings(RestUtil)
      val finalList = contentRatingList.filter(f => contentList.contains(f.contentId))
      JobLogger.log("content-ids: " + contentList.size + " rating list: " + contentRatingList.size + " filtered list: " + finalList.size, None, Level.INFO)
      sc.parallelize(finalList)
  }

  override def postProcess(data: RDD[ContentRatingResult], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ContentRatingResult] = {
      val baseURL = AppConf.getConfig("lp.system.update.base.url")
      if (data.count() > 0) {
          data.foreach { f =>
              val response = publishRatingToContentModel(f.contentId, f.numOfTimesRated, f.averageRating, baseURL, RestUtil)
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
      if(startDate.equals(endDate)) endDate = new DateTime(endDate).plusDays(1).toString("yyyy-MM-dd")
      val contentRequest = AppConf.getConfig("druid.unique.content.query").format(new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"))
      val contentResponse = restUtil.post[List[Map[String, AnyRef]]](apiURL, contentRequest)
      contentResponse.map(x => x.getOrElse("Id", "").toString)
  }

  def getContentRatings(restUtil: HTTPClient): List[ContentRatingResult] = {
      val apiURL = AppConf.getConfig("druid.sql.host")
      val ratingRequest = AppConf.getConfig("druid.content.rating.query")
      val contentRatingResponse = restUtil.post[List[Map[String, AnyRef]]](apiURL, ratingRequest)
      contentRatingResponse.map(x => ContentRatingResult(x.getOrElse("ContentId", "").toString, x.getOrElse("Number of Ratings", 0.0).asInstanceOf[Number].longValue(), x.getOrElse("AverageRating", 0.0).asInstanceOf[Number].doubleValue()))
  }

  def publishRatingToContentModel(contentId: String, numOfTimesRated: Long, avgRating: Double, baseURL: String, restUtil: HTTPClient): Response = {
      val systemUpdateURL = s"$baseURL/$contentId"
      val request =
          s"""
             |{
             |  "request": {
             |    "content": {
             |      "me_totalRatingsCount": $numOfTimesRated,
             |      "me_averageRating": $avgRating
             |    }
             |  }
             |}
               """.stripMargin
      val response = restUtil.patch[String](systemUpdateURL, request)
      JSONUtils.deserialize[Response](response)
  }
}
