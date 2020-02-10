package org.ekstep.analytics.model.report

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import io.circe._
import io.circe.parser._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils}
import org.ekstep.analytics.model.{ReportConfig, SparkSpec}
import org.ekstep.analytics.util.{CourseDetails, CourseReport, EmbeddedCassandra}
import org.scalamock.scalatest.MockFactory
import org.sunbird.cloud.storage.BaseStorageService

import scala.concurrent.Future
import scala.io.Source

class TestCourseConsumptionModel extends SparkSpec(null) with MockFactory{

  implicit val mockCourseReport: CourseReport = mock[CourseReport]
  val mockRestUtil = mock[HTTPClient]
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql")
  }

  "CourseConsumptionModel" should "execute Course consumption model successfully" in {
    implicit val sqlContext = new SQLContext(sc)
    implicit val mockFc = mock[FrameworkContext]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes();

    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.CourseConsumptionModel","modelParams":{"esConfig":{"request":{"filters":{"objectType":["Content"],"contentType":["Course"],"identifier":[],"status":["Live"]}}},"reportConfig":{"id":"tpd_metrics","labels":{"date":"Date","status":"Batch Status","timespent":"Timespent in mins","courseName":"Course Name","batchName":"Batch Name"},"dateRange":{"staticInterval":"2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"totalCoursePlays","label":"Total Course Plays (in mins)","druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}}],"output":[{"type":"csv","metrics":["timespent"],"dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"queryType":"groupBy"},"key":"druid-reports/","filePath":"src/test/resources/","bucket":"test-container","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    //Mock for compositeSearch
    val userdata = JSONUtils.deserialize[CourseDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/course-report/liveCourse.json")).getLines().mkString).result.content

    import sqlContext.implicits._
    val userDF = userdata.toDF("channel", "identifier", "courseName")
    (mockCourseReport.getCourse(_: Map[String, AnyRef])(_: SparkContext)).expects(jobConfig.get, *).returns(userDF).anyNumberOfTimes()

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "sum__edata_time_spent": 5.46,
        |    "timespent": 0.091,
        |    "batchId": "0127419590263029761308",
        |    "courseId": "do_112470675618004992181",
        |    "userId": "95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null);
    val results = List(DruidResult.apply(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC), doc));
    val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val resultRDD = CourseConsumptionModel.execute(sc.emptyRDD, jobConfig)
    val result = resultRDD.collect()

    resultRDD.count() should be (1)
    result.map{f =>
      f.batchName should be ("testCourseBatch")
      f.courseName should be ("29 course")
      f.timespent.get should be (0.09)}

    val configMap = jobConfig.get("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportId = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap)).id

    val slug = result.map(f => f.slug).toList
    val reportName = result.map(_.reportName).toList.head
    slug.head should be ("MPSlug")
    val filePath = jobConfig.get.get("filePath").get.asInstanceOf[String]
    val key = jobConfig.get.get("key").get.asInstanceOf[String]
    val outDir = filePath + key + "renamed/" + reportId + "/" + slug.head + "/"
    outDir should be ("src/test/resources/druid-reports/renamed/tpd_metrics/MPSlug/")
  }

  it should "execute Course consumption model successfully and dispatch to azure even if output type is other than csv" in {
    implicit val sqlContext = new SQLContext(sc)
    implicit val mockFc = mock[FrameworkContext]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes();

    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.CourseConsumptionModel","modelParams":{"esConfig":{"request":{"filters":{"objectType":["Content"],"contentType":["Course"],"identifier":[],"status":["Live"]}}},"reportConfig":{"id":"tpd_metrics","labels":{"date":"Date","status":"Batch Status","timespent":"Timespent in mins","courseName":"Course Name","batchName":"Batch Name"},"dateRange":{"staticInterval":"2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"totalCoursePlays","label":"Total Course Plays (in mins)","druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}}],"output":[{"type":"json","metrics":["timespent"],"dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"queryType":"groupBy"},"key":"druid-reports/","filePath":"src/test/resources/","bucket":"test-container","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    //Mock for compositeSearch
    val userdata = JSONUtils.deserialize[CourseDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/course-report/liveCourse.json")).getLines().mkString).result.content

    import sqlContext.implicits._
    val userDF = userdata.toDF("channel", "identifier", "courseName")
    (mockCourseReport.getCourse(_: Map[String, AnyRef])(_: SparkContext)).expects(jobConfig.get, *).returns(userDF).anyNumberOfTimes()

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "sum__edata_time_spent": 5.46,
        |    "timespent": 0.091,
        |    "batchId": "0127419590263029761308",
        |    "courseId": "do_112470675618004992181",
        |    "userId": "95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null);
    val results = List(DruidResult.apply(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC), doc));
    val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val resultRDD = CourseConsumptionModel.execute(sc.emptyRDD, jobConfig)
    val result = resultRDD.collect()

    resultRDD.count() should be (1)
    result.map{f =>
      f.batchName should be ("testCourseBatch")
      f.courseName should be ("29 course")
      f.timespent.get should be (0.09)}

    val configMap = jobConfig.get("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportId = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap)).id

    val slug = result.map(f => f.slug).toList
    val reportName = result.map(_.reportName).toList.head
    slug.head should be ("MPSlug")
    val filePath = jobConfig.get.get("filePath").get.asInstanceOf[String]
    val key = jobConfig.get.get("key").get.asInstanceOf[String]
    val outDir = filePath + key + "renamed/" + reportId + "/" + slug.head + "/"
    outDir should be ("src/test/resources/druid-reports/renamed/tpd_metrics/MPSlug/")
  }

  it should "execute successfully and provide null if there are no common keys" in {
    implicit val sqlContext = new SQLContext(sc)
    implicit val mockFc = mock[FrameworkContext]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes();

    val config = """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.CourseConsumptionModel","modelParams":{"esConfig":{"request":{"filters":{"objectType":["Content"],"contentType":["Course"],"identifier":[],"status":["Live"]}}},"reportConfig":{"id":"tpd_metrics","labels":{"date":"Date","status":"Batch Status","timespent":"Timespent in mins","courseName":"Course Name","batchName":"Batch Name"},"dateRange":{"staticInterval":"2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"totalCoursePlays","label":"Total Course Plays (in mins)","druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}}],"output":[{"type":"csv","metrics":["timespent"],"dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"queryType":"groupBy"},"key":"druid-reports/","filePath":"src/test/resources/","bucket":"test-container","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams

    //Mock for compositeSearch
    val userdata = JSONUtils.deserialize[CourseDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/course-report/liveCourse.json")).getLines().mkString).result.content

    import sqlContext.implicits._
    val userDF = userdata.toDF("channel", "identifier", "courseName")
    (mockCourseReport.getCourse(_: Map[String, AnyRef])(_: SparkContext)).expects(jobConfig.get, *).returns(userDF).anyNumberOfTimes()

    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "sum__edata_time_spent": 5.46,
        |    "timespent": 0.00,
        |    "batchId": "0127419590263029761321",
        |    "courseId": "do_112470675618004992131",
        |    "userId": "95e4942d-cbe8-477d-aebd-ad8e6de4bfc8"
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null);
    val results = List(DruidResult.apply(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC), doc));
    val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    val result = CourseConsumptionModel.execute(sc.emptyRDD, jobConfig)

    result.count() should be (1)
    result.collect().map{f =>
      f.batchName should be ("")
      f.courseName should be ("")
      f.timespent.get should be (0.0)
      f.slug should be ("unknown")
    }
  }

  it should "give error if there is no data for output" in {
    val data = Seq.empty[CourseConsumptionOutput]
    val dataRDD = sc.parallelize(data)
    val config= """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.CourseConsumptionModel","modelParams":{"esConfig":{"request":{"filters":{"objectType":["Content"],"contentType":["Course"],"identifier":[],"status":["Live"]}}},"reportConfig":{"id":"tpd_metrics","labels":{"date":"Date","status":"Batch Status","timespent":"Timespent in mins","courseName":"Course Name","batchName":"Batch Name"},"dateRange":{"staticInterval":"2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"totalCoursePlays","label":"Total Course Plays (in mins)","druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}}],"output":[{"type":"csv","metrics":["timespent"],"dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"queryType":"groupBy"},"key":"druid-reports/","filePath":"src/test/resources/","bucket":"test-container","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams
    implicit val mockFc = mock[FrameworkContext]
    CourseConsumptionModel.postProcess(dataRDD, jobConfig.get)
  }
}