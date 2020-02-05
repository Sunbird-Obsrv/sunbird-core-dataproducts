package org.ekstep.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.{ReportConfig, SparkSpec}
import org.ekstep.analytics.util._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.cloud.storage.BaseStorageService

import scala.collection.mutable.Buffer
import scala.io.Source

class TestCourseEnrollmentModel extends SparkSpec with Matchers with MockFactory {

  implicit val spark: SparkSession = getSparkSession()
  implicit val sqlContext = new SQLContext(sc)
  implicit val mockCourseReport: CourseReport = mock[CourseReport]

  var courseBatchDF: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    val cbMapping = sc.textFile("src/test/resources/course-report/coursebatch_mapping.json", 1).collect().head
    EmbeddedES.start(Array
    (EsIndex("course-batch",Option("search"), Option(cbMapping),None)))

    EmbeddedES.loadData("course-batch", "search", Buffer(
      """{"courseId":"do_112470675618004992181","participantCount":2,"completedCount":0,"batchId":"0127462617892044804"}""",
      """{"courseId":"0128448115803914244","participantCount":3,"completedCount":3,"batchId":"0127419590263029761308"}""",
      """{"courseId":"05ffe180caa164f56ac193964c5816d4","participantCount":4,"completedCount":3,"batchId":"01273776766975180837"}"""))

    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql")
  }

  override def afterAll() {
    super.afterAll()
    EmbeddedES.stop()
  }

  "CourseEnrollmentModel" should "execute Course Enrollment model" in {
    implicit val mockFc = mock[FrameworkContext]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes()
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()

    val config = s"""{
                    |	"reportConfig": {
                    |		"id": "tpd_metrics",
                    |    "metrics" : [],
                    |		"labels": {
                    |			"completionCount": "Completion Count",
                    |			"status": "Status",
                    |			"enrollmentCount": "Enrollment Count",
                    |			"courseName": "Course Name",
                    |			"batchName": "Batch Name"
                    |		},
                    |		"output": [{
                    |			"type": "csv",
                    |			"dims": ["identifier", "channel", "name"],
                    |			"fileParameters": ["id", "dims"]
                    |		}]
                    |	},
                    | "esConfig": {
                    | "request": {
                    |        "filters":{
                    |            "objectType": ["Content"],
                    |            "contentType": ["Course"],
                    |            "identifier": [],
                    |            "status": ["Live"]
                    |        },
                    |        "limit": 10000
                    |    }
                    | },
                    |	"key": "druid-reports/",
                    |	"filePath": "druid-reports/",
                    |	"container": "test-container",
                    |	"folderPrefix": ["slug", "reportName"],
                    | "store": "local"
                    |}""".stripMargin
    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config)
    //Mock for compositeSearch
    val userdata = JSONUtils.deserialize[CourseDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/course-report/liveCourse.json")).getLines().mkString).result.content

    import sqlContext.implicits._
    val userDF = userdata.toDF("channel", "identifier", "courseName")
    (mockCourseReport.getCourse(_: Map[String, AnyRef])(_: SparkContext)).expects(jobConfig, *).returns(userDF).anyNumberOfTimes()

    val resultRDD = CourseEnrollmentModel.execute(sc.emptyRDD, Option(jobConfig))
    val result = resultRDD.collect()

    resultRDD.count() should be(4)

    result.map(f => {
      f.completionCount should be(0)
    })

    val configMap = jobConfig.get("reportConfig").get.asInstanceOf[Map[String,AnyRef]]
    val reportId = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap)).id

    val slug = result.map(f => f.slug).toList
    val reportName = result.map(_.reportName).toList.head
    val filePath = jobConfig.get("filePath").get.asInstanceOf[String]
    val key = jobConfig.get("key").get.asInstanceOf[String]
    val outDir = filePath + key + "renamed/" + reportId + "/" + slug.head + "/"
  }

  it should "give error if there is no data for output" in {
    val data = Seq.empty[CourseEnrollmentOutput]
    val dataRDD = sc.parallelize(data)
    val config= """{"search":{"type":"none"},"model":"org.ekstep.analytics.model.CourseConsumptionModel","modelParams":{"esConfig":{"request":{"filters":{"objectType":["Content"],"contentType":["Course"],"identifier":[],"status":["Live"]}}},"reportConfig":{"id":"tpd_metrics","labels":{"date":"Date","status":"Batch Status","timespent":"Timespent in mins","courseName":"Course Name","batchName":"Batch Name"},"dateRange":{"staticInterval":"2019-09-08T00:00:00+00:00/2019-09-09T00:00:00+00:00","granularity":"all"},"metrics":[{"metric":"totalCoursePlays","label":"Total Course Plays (in mins)","druidQuery":{"queryType":"groupBy","dataSource":"summary-events","intervals":"2019-09-08/2019-09-09","aggregations":[{"name":"sum__edata_time_spent","type":"doubleSum","fieldName":"edata_time_spent"}],"dimensions":[{"fieldName":"object_rollup_l1","aliasName":"courseId"},{"fieldName":"uid","aliasName":"userId"},{"fieldName":"context_cdata_id","aliasName":"batchId"}],"filters":[{"type":"equals","dimension":"eid","value":"ME_WORKFLOW_SUMMARY"},{"type":"in","dimension":"dimensions_pdata_id","values":["dev.sunbird.app","dev.sunbird.portal"]},{"type":"equals","dimension":"dimensions_type","value":"content"},{"type":"equals","dimension":"dimensions_mode","value":"play"},{"type":"equals","dimension":"context_cdata_type","value":"batch"}],"postAggregation":[{"type":"arithmetic","name":"timespent","fields":{"leftField":"sum__edata_time_spent","rightField":60,"rightFieldType":"constant"},"fn":"/"}],"descending":"false"}}],"output":[{"type":"csv","metrics":["timespent"],"dims":["identifier","channel","name"],"fileParameters":["id","dims"]}],"queryType":"groupBy"},"key":"druid-reports/","filePath":"src/test/resources/","bucket":"test-container","folderPrefix":["slug","reportName"]},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"TPD Course Consumption Metrics Model","deviceMapping":false}"""
    val jobConfig = JSONUtils.deserialize[JobConfig](config).modelParams
    implicit val mockFc = mock[FrameworkContext]
    CourseEnrollmentModel.postProcess(dataRDD, jobConfig.get)
  }

  it should "execute Course Enrollment model and dispatch to azure even if type is other than csv" in {
    implicit val mockFc = mock[FrameworkContext]

    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String)).expects("azure").returns(mockStorageService).anyNumberOfTimes()
    (mockStorageService.upload (_: String, _: String, _: String, _: Option[Boolean], _: Option[Int], _: Option[Int], _: Option[Int])).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()

    val config = s"""{
                    |	"reportConfig": {
                    |		"id": "tpd_metrics",
                    |    "metrics" : [],
                    |		"labels": {
                    |			"completionCount": "Completion Count",
                    |			"status": "Status",
                    |			"enrollmentCount": "Enrollment Count",
                    |			"courseName": "Course Name",
                    |			"batchName": "Batch Name"
                    |		},
                    |		"output": [{
                    |			"type": "json",
                    |			"dims": ["identifier", "channel", "name"],
                    |			"fileParameters": ["id", "dims"]
                    |		}]
                    |	},
                    | "esConfig": {
                    | "request": {
                    |        "filters":{
                    |            "objectType": ["Content"],
                    |            "contentType": ["Course"],
                    |            "identifier": [],
                    |            "status": ["Live"]
                    |        },
                    |        "limit": 10000
                    |    }
                    | },
                    |	"key": "druid-reports/",
                    |	"filePath": "src/test/resources/",
                    |	"bucket": "test-container",
                    |	"folderPrefix": ["slug", "reportName"]
                    |}""".stripMargin
    val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](config)
    //Mock for compositeSearch
    val userdata = JSONUtils.deserialize[CourseDetails](Source.fromInputStream
    (getClass.getResourceAsStream("/course-report/liveCourse.json")).getLines().mkString).result.content

    import sqlContext.implicits._
    val userDF = userdata.toDF("channel", "identifier", "courseName")
    (mockCourseReport.getCourse(_: Map[String, AnyRef])(_: SparkContext)).expects(jobConfig, *).returns(userDF).anyNumberOfTimes()

    val resultRDD = CourseEnrollmentModel.execute(sc.emptyRDD, Option(jobConfig))
    val result = resultRDD.collect()

    resultRDD.count() should be(4)

    result.map(f => {
      f.completionCount should be(0)
    })

    val configMap = jobConfig.get("reportConfig").get.asInstanceOf[Map[String,AnyRef]]
    val reportId = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap)).id

    val slug = result.map(f => f.slug).toList
    val reportName = result.map(_.reportName).toList.head
    val filePath = jobConfig.get("filePath").get.asInstanceOf[String]
    val key = jobConfig.get("key").get.asInstanceOf[String]
    val outDir = filePath + key + "renamed/" + reportId + "/" + slug.head + "/"
  }
}