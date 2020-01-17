package org.ekstep.analytics.updater

/**
  * Ref:Design wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794198025/Design+Brainstorm+Data+structure+for+capturing+dashboard+portal+metrics
  * Ref:Implementation wiki link: https://project-sunbird.atlassian.net/wiki/spaces/SBDES/pages/794099772/Data+Product+Dashboard+summariser+-+Cumulative
  *
  * @author Manjunath Davanam <manjunathd@ilimi.in>
  */

import com.datastax.spark.connector._
import javax.ws.rs.core.HttpHeaders
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SQLContext}
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.ekstep.analytics.util.Constants

import scala.util.Try


case class WorkFlowUsageMetrics(noOfUniqueDevices: Long, totalContentPlaySessions: Long, totalTimeSpent: Double, totalContentPublished: ContentPublishedList) extends AlgoOutput with Output with AlgoInput

case class PortalMetrics(eid: String, ets: Long, syncts: Long, metrics_summary: Option[WorkFlowUsageMetrics]) extends AlgoOutput with Output

case class ContentPublishedList(count: Int, language_publisher_breakdown: List[LanguageByPublisher])

case class LanguageByPublisher(publisher: String, languages: List[Language])

case class Language(id: String, count: Double)

case class ESResponse(aggregations: Aggregations)

case class Aggregations(publisher_agg: AggregationResult)

case class AggregationResult(buckets: List[Buckets])

case class Buckets(key: String, doc_count: Double, language_agg: AggregationResult)

case class OrgResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: OrgResult)

case class OrgResult(response: ContentList)

case class ContentList(count: Long, content: List[OrgProps])

case class OrgProps(orgName: String, hashTagId: String, rootOrgId: String)

case class DeviceProfile(device_id: String)

object UpdatePortalMetrics extends IBatchModelTemplate[DerivedEvent, DerivedEvent, WorkFlowUsageMetrics, PortalMetrics]
  with Serializable {

  val className = "org.ekstep.analytics.updater.UpdatePortalMetrics"

  private val EVENT_ID: String = "ME_PORTAL_CUMULATIVE_METRICS"

  override def name: String = "UpdatePortalMetrics"

  val db = AppConf.getConfig("postgres.db")
  val url = AppConf.getConfig("postgres.url") + s"$db"
  val connProperties = CommonUtil.getPostgresConnectionProps

  /**
    * preProcess which will fetch the `workflow_usage_summary` Event data from the Cassandra Database.
    *
    * @param data   - RDD Event Data(Empty RDD event)
    * @param config - Configurations to run preProcess
    * @param sc     - SparkContext
    * @return -     DerivedEvent
    */
  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DerivedEvent] = {
    data
  }

  /**
    *
    * @param data   - RDD Workflow summary event data
    * @param config - Configurations to algorithm
    * @param sc     - Spark context
    * @return - DashBoardSummary ->(uniqueDevices, totalContentPlayTime, totalTimeSpent,)
    */
  override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef]) (implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkFlowUsageMetrics] = {
    implicit val sqlContext = new SQLContext(sc)

    val publisherByLanguageList = getLanguageAndPublisherList()
    val totalContentPublished = Try(ContentAdapter.getPublishedContentList().count).getOrElse(0)

    val encoder = Encoders.product[DeviceProfile]
    val noOfUniqueDevices = sqlContext.sparkSession.read.jdbc(url, Constants.DEVICE_PROFILE_TABLE, connProperties).as[DeviceProfile](encoder).rdd
      .map(_.device_id).distinct().count()

    val metrics =
      sc.cassandraTable[WorkFlowUsageMetricsAlgoOutput](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY)
        .map(event => {
          (event.total_timespent, event.total_content_play_sessions)
        })
    val totalTimeSpent = metrics.map(_._1).sum()
    val totalContentPlaySessions = metrics.map(_._2).sum().toLong
    sc.parallelize(Array(WorkFlowUsageMetrics(noOfUniqueDevices, totalContentPlaySessions,
      CommonUtil.roundDouble(totalTimeSpent / 3600, 2),
      ContentPublishedList(totalContentPublished, publisherByLanguageList))))
  }

  /**
    *
    * @param data   - RDD DashboardSummary Event
    * @param config - Configurations to run postprocess method
    * @param sc     - Spark context
    * @return - ME_PORTAL_CUMULATIVE_METRICS MeasuredEvents
    */
  override def postProcess(data: RDD[WorkFlowUsageMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[PortalMetrics] = {
    val record = data.first()
    val measures = WorkFlowUsageMetrics(record.noOfUniqueDevices, record.totalContentPlaySessions, record.totalTimeSpent, record.totalContentPublished)
    val metrics = PortalMetrics(EVENT_ID, System.currentTimeMillis(), System.currentTimeMillis(), Some(measures))
    if (config.getOrElse("dispatch", false).asInstanceOf[Boolean]) {
      AzureDispatcher.dispatch(Array(JSONUtils.serialize(metrics)), config)
    }
    sc.parallelize(Array(metrics))
  }

  private def getLanguageAndPublisherList(): List[LanguageByPublisher] = {
    val apiURL = Constants.ELASTIC_SEARCH_SERVICE_ENDPOINT + "/" + Constants.ELASTIC_SEARCH_INDEX_COMPOSITESEARCH_NAME + "/_search"
    println("APIURL", apiURL);
    val request =
      s"""
         |{
         |  "_source":false,
         |  "query":{
         |    "bool":{
         |      "must":[
         |        {
         |          "match":{
         |            "status":{
         |              "query":"Live",
         |              "operator":"AND",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        }
         |      ],
         |      "should":[
         |        {
         |          "match":{
         |            "objectType":{
         |              "query":"Content",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        },
         |        {
         |          "match":{
         |            "objectType":{
         |              "query":"ContentImage",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        },
         |        {
         |          "match":{
         |            "contentType":{
         |              "query":"Resource",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        },
         |        {
         |          "match":{
         |            "contentType":{
         |              "query":"Collection",
         |              "operator":"OR",
         |              "lenient":false,
         |              "zero_terms_query":"NONE"
         |            }
         |          }
         |        }
         |      ]
         |    }
         |  },
         |  "aggs":{
         |    "publisher_agg":{
         |      "terms":{
         |        "field":"createdFor.raw",
         |        "size":1000
         |      },
         |      "aggs":{
         |        "language_agg":{
         |          "terms":{
         |            "field":"language.raw",
         |            "size":1000
         |          }
         |        }
         |      }
         |    }
         |  }
         |}
       """.stripMargin
    val response = RestUtil.post[ESResponse](apiURL, request)
    val orgResult = orgSearch()
    if(null != response) {
      response.aggregations.publisher_agg.buckets.map(publisherBucket => {
        val languages = publisherBucket.language_agg.buckets.map(languageBucket => {
          Language(languageBucket.key, languageBucket.doc_count)
        })
        orgResult.result.response.content.map(org => {
          if (org.hashTagId == publisherBucket.key) {
            LanguageByPublisher(org.orgName, languages)
          } else {
            LanguageByPublisher("", List()) // Return empty publisher list
          }
        }).filter(_.publisher.nonEmpty)
      })
    } else {
      List()
    }
    
  }.flatMap(f=>f)

  private def orgSearch(): OrgResponse = {
    val request =
      s"""
         |{
         |  "request":{
         |    "filters":{
         |      "isRootOrg":true
         |    },
         |    "limit":1000,
         |    "fields":[
         |      "orgName",
         |      "rootOrgId",
         |      "hashTagId"
         |    ]
         |  }
         |}
       """.stripMargin
    val requestHeaders = Map(HttpHeaders.AUTHORIZATION -> Constants.ORG_SEARCH_API_KEY)
    RestUtil.post[OrgResponse](Constants.ORG_SEARCH_URL, request, Some(requestHeaders))
  }
}