package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework.Params
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.util.Constants

case class ContentModel(id: String, subject: List[String], contentType: String, languageCode: List[String], gradeList: List[String] = List[String]());
case class ContentResult(count: Int, content: Option[Array[Map[String, AnyRef]]])
case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult)

trait ContentFetcher {
  def getPublishedContentList: ContentResult
}
/**
 * @author Santhosh
 */
object ContentAdapter extends BaseAdapter with ContentFetcher {

  val relations = Array("concepts", "tags");

  implicit val className = "org.ekstep.analytics.adapter.ContentAdapter"
  /**
   * Which is used to get the total published contents list
   * @return ContentResult
   */
  def getPublishedContentList(): ContentResult = {
    val request =
      s"""
               |{
               |    "request": {
               |        "filters":{
               |          "contentType": "Resource"
               |        },
               |        "fields": ["identifier", "objectType", "resourceType"]
               |    }
               |}
             """.stripMargin
    RestUtil.post[ContentResponse](Constants.COMPOSITE_SEARCH_URL, request).result
  }
}