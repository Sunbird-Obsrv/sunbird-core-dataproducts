package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.util.HTTPClient
import org.ekstep.analytics.model.SparkSpec
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.framework.FrameworkContext

class TestUpdateContentRating  extends SparkSpec(null) with MockFactory {

    implicit val fc = new FrameworkContext();
    "UpdateContentRating" should "get content list which are rated in given time" in {

        val startDate = new DateTime().minusDays(1).toString("yyyy-MM-dd")
        val endDate = new DateTime().toString("yyyy-MM-dd")
        val mockRestUtil = mock[HTTPClient]
        (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
          .expects("http://localhost:8082/druid/v2/sql/", "{\"query\":\"SELECT DISTINCT \\\"object_id\\\" AS \\\"Id\\\"\\nFROM \\\"druid\\\".\\\"%s\\\" WHERE \\\"eid\\\" = 'FEEDBACK' AND \\\"__time\\\" BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s' \"}".format("telemetry-events", new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")), None, manifest[List[Map[String, AnyRef]]])
          .returns(List(Map("ContentId" -> "test-1"), Map("ContentId" -> "test-2")))

        val contentIds = UpdateContentRating.getRatedContents(Map("startDate" -> startDate.asInstanceOf[AnyRef], "endDate" -> endDate.asInstanceOf[AnyRef]), mockRestUtil)
        contentIds.size should be(2)
    }

    it should "alter end date for replay scenario" in {

        val startDate = new DateTime().minusDays(1).toString("yyyy-MM-dd")
        val endDate = new DateTime().toString("yyyy-MM-dd")
        val mockRestUtil = mock[HTTPClient]
        (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
          .expects("http://localhost:8082/druid/v2/sql/", "{\"query\":\"SELECT DISTINCT \\\"object_id\\\" AS \\\"Id\\\"\\nFROM \\\"druid\\\".\\\"%s\\\" WHERE \\\"eid\\\" = 'FEEDBACK' AND \\\"__time\\\" BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s' \"}".format("telemetry-events", new DateTime(startDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss"), new DateTime(endDate).withTimeAtStartOfDay().toString("yyyy-MM-dd HH:mm:ss")), None, manifest[List[Map[String, AnyRef]]])
          .returns(List(Map("ContentId" -> "test-1"), Map("ContentId" -> "test-2")))

        val contentIds = UpdateContentRating.getRatedContents(Map("startDate" -> startDate.asInstanceOf[AnyRef], "endDate" -> startDate.asInstanceOf[AnyRef]), mockRestUtil)
        contentIds.size should be(2)
    }

    it should "get all content ratings" in {

        val mockRestUtil = mock[HTTPClient]
        (mockRestUtil.post[List[Map[String, AnyRef]]](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[List[Map[String, AnyRef]]]))
          .expects("http://localhost:8082/druid/v2/sql/", "{\"query\": \"SELECT \\\"object_id\\\" AS ContentId, COUNT(*) AS \\\"Number of Ratings\\\", SUM(edata_rating) AS \\\"Total Ratings\\\", SUM(edata_rating)/COUNT(*) AS \\\"AverageRating\\\" FROM \\\"druid\\\".\\\"telemetry-events\\\" WHERE \\\"eid\\\" = 'FEEDBACK' GROUP BY \\\"object_id\\\"\"}", None, manifest[List[Map[String, AnyRef]]])
          .returns(List(Map("ContentId" -> "test-1", "Total Ratings" -> 25.asInstanceOf[AnyRef], "Number of Ratings" -> 5.asInstanceOf[AnyRef], "AverageRating" -> 5.asInstanceOf[AnyRef]), Map("ContentId" -> "test-2", "AverageRating" -> 3.asInstanceOf[AnyRef])))

        val contentRatings = UpdateContentRating.getContentRatings(mockRestUtil)
        contentRatings.size should be(2)
        contentRatings(0).contentId should be("test-1")
        contentRatings(0).averageRating should be(5)
        contentRatings(0).numOfTimesRated should be(5)
        contentRatings(1).contentId should be("test-2")
        contentRatings(1).averageRating should be(3)
        contentRatings(1).numOfTimesRated should be(0)
    }

    ignore should "check for system update API call" in {

        val mockRestUtil = mock[HTTPClient]
        val systemUpdateURL = "http://localhost:8080/learning-service/system/v3/content/update/test-1"
        val request =
            s"""
               |{
               |  "request": {
               |    "content": {
               |      "me_totalRatingsCount": 5,
               |      "me_averageRating": 5.0
               |    }
               |  }
               |}
             """.stripMargin
        val mockResponse =
          s"""
             |{
             |    "id": "ekstep.learning.system.content.update",
             |    "ver": "1.0",
             |    "ts": "2019-05-02T12:20:17ZZ",
             |    "params": {
             |        "resmsgid": "622ade80-e22a-4cc1-8683-d002babe9ae6",
             |        "msgid": null,
             |        "err": null,
             |        "status": "successful",
             |        "errmsg": null
             |    },
             |    "responseCode": "OK",
             |    "result": {
             |        "node_id": "test-1",
             |        "versionKey": "1554515533414"
             |    }
             |}
           """.stripMargin

        (mockRestUtil.patch[String](_: String, _: String, _: Option[Map[String, String]])(_: Manifest[String]))
            .expects(systemUpdateURL, request, None, manifest[String])
            .returns(mockResponse)

        val response = UpdateContentRating.publishRatingToContentModel("test-1", 5, 5.0, "http://localhost:8080/learning-service/system/v3/content/update", mockRestUtil)
        println(response)
        response.params.status.getOrElse("") should be("successful")
        response.result.getOrElse("node_id", "") should be("test-1")

    }

    ignore should "check for system update API call failure" in {

        val mockRestUtil = mock[HTTPClient]
        val systemUpdateURL = "http://localhost:8080/learning-service/system/v3/content/update/test-1"
        val request =
            s"""
               |{
               |  "request": {
               |    "content": {
               |      "me_totalRatingsCount": 5,
               |      "me_averageRating": 5.0
               |    }
               |  }
               |}
             """.stripMargin
        val mockResponse =
            s"""
               |{
               |    "id": "ekstep.learning.system.content.update",
               |    "ver": "1.0",
               |    "ts": "2019-05-14T14:43:36ZZ",
               |    "params": {
               |        "resmsgid": "c5369a14-eb6e-4eb0-9288-d4cb272d59b9",
               |        "msgid": null,
               |        "err": "ERR_GRAPH_UPDATE_NODE_VALIDATION_FAILED",
               |        "status": "failed",
               |        "errmsg": "Node Metadata validation failed"
               |    },
               |    "responseCode": "CLIENT_ERROR",
               |    "result": {
               |        "messages": [
               |            "Please provide framework.",
               |            "Metadata contentType should be one of: [Resource, Collection, TextBook, LessonPlan, Course, Template, Asset, Plugin, LessonPlanUnit, CourseUnit, TextBookUnit, TeachingMethod, PedagogyFlow]",
               |            "Metadata resourceType should be one of: [Read, Learn, Teach, Play, Test, Practice, Experiment, Collection, Book, Lesson Plan, Course, Theory, Worksheet, Practical]"
               |        ],
               |        "node_id": "org.ekstep.jun16.story.test05"
               |    }
               |}
           """.stripMargin


        (mockRestUtil.patch[String](_: String, _: String, _: Option[Map[String, String]])( _: Manifest[String]))
          .expects(systemUpdateURL, request, None, manifest[String])
          .returns(mockResponse)

        val response = UpdateContentRating.publishRatingToContentModel("test-1", 5, 5.0, "http://localhost:8080/learning-service/system/v3/content/update", mockRestUtil)
        println(response)
        response.params.status.getOrElse("") should be("failed")
        response.result.getOrElse("node_id", "") should be("org.ekstep.jun16.story.test05")

    }

}
