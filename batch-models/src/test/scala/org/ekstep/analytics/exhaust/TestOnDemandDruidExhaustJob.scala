package org.ekstep.analytics.exhaust

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.{DruidFilter, DruidQueryModel, FrameworkContext, IJob, JobConfig, JobContext, StorageConfig}
import org.ekstep.analytics.framework.util.{CommonUtil, HadoopFileUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.{BaseDruidQueryProcessor, BaseSpec, EmbeddedPostgresql}
import cats.syntax.either._
import com.ing.wbaa.druid._
import com.ing.wbaa.druid.client.DruidClient
import io.circe.Json
import io.circe.parser._
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.exhaust.OnDemandDruidExhaustJob.execute
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.job.JobFactory
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory

import scala.concurrent.Future
import org.scalatest.{BeforeAndAfterAll, Matchers}
import org.sunbird.cloud.storage.BaseStorageService

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.immutable.{List, Map}

class TestOnDemandDruidExhaustJob extends BaseSpec with Matchers with BeforeAndAfterAll with MockFactory with BaseReportsJob {
  val jobRequestTable = "job_request"
  implicit var spark: SparkSession = _
  implicit var sc: SparkContext = _
  val outputLocation = AppConf.getConfig("collection.exhaust.store.prefix")
  implicit var sqlContext : SQLContext = _
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val fc = mock[FrameworkContext]
  val hadoopFileUtil = new HadoopFileUtil()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();
    sc = spark.sparkContext
    sqlContext = new SQLContext(sc)
    EmbeddedPostgresql.start()
    EmbeddedPostgresql.createJobRequestTable()
    EmbeddedPostgresql.createDatasetMetadataTable()
  }

  override def afterAll() : Unit = {
    super.afterAll()
    new HadoopFileUtil().delete(spark.sparkContext.hadoopConfiguration, outputLocation)
    spark.close()
    EmbeddedPostgresql.close()
  }

  def getDate(pattern: String): SimpleDateFormat = {
    new SimpleDateFormat(pattern)
  }
  val reportDate = getDate("yyyyMMdd").format(Calendar.getInstance().getTime())

  it should "return PostgresException" in {

    val job = JobFactory.getJob("druid-dataset")
    job should be(OnDemandDruidExhaustJob)
    job.isInstanceOf[IJob] should be(true)
  }

  "TestOnDemandDruidExhaustJob" should "generate report with correct values" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f",
            "area_of_improvement":"Education Leader","school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI",
            "program_name":"Prerak Head Teacher of the Block 19-20","state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"completed"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    var name = OnDemandDruidExhaustJob.name()

    val submissionDate = DateTime.now().toString("yyyy-MM-dd")
    val druidQueryConf = """{"id":"ml-task-detail-exhaust","queryType":"scan","dateRange":{"interval":{"startDate":"1901-01-01","endDate":"2101-01-01"},"granularity":"all"},"metrics":[{"metric":"total_content_plays_on_portal","label":"total_content_plays_on_portal","druidQuery":{"queryType":"scan","dataSource":"sl-project","intervals":"1901-01-01T00:00+00:00/2101-01-01T00:00:00+00:00","columns":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project"],"filters":[{"type":"equals","dimension":"private_program","value":"false"},{"type":"equals","dimension":"sub_task_deleted_flag","value":"false"},{"type":"equals","dimension":"task_deleted_flag","value":"false"},{"type":"equals","dimension":"project_deleted_flag","value":"false"},{"type":"equals","dimension":"program_id","value":"602512d8e6aefa27d9629bc3"},{"type":"equals","dimension":"solution_id","value":"602a19d840d02028f3af00f0"}]}}],"labels":{"createdBy":"UUID","designation":"Role","state_name":"Declared State","district_name":"District","block_name":"Block","school_name":"School Name","school_externalId":"School ID","organisation_name":"Organisation Name","program_name":"Program Name","program_externalId":"Program ID","project_id":"Project ID","project_title_editable":"Project Title","project_description":"Project Objective","area_of_improvement":"Category","project_duration":"Project Duration","tasks":"Tasks","sub_task":"Sub-Tasks","task_evidence":"Evidence","task_remarks":"Remarks","status_of_project":"Project Status"},"output":[{"type":"csv","metrics":["createdBy","designation","state_name","district_name","block_name","school_name","school_externalId","organisation_name","program_name","program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project"],"fileParameters":["id","dims"],"zip":false,"dims":["date"],"label":""}]}"""
    EmbeddedPostgresql.execute(
      s"""insert into dataset_metadata ("dataset_id", "dataset_sub_id", "dataset_config", "visibility", "dataset_type", "version",
          "authorized_roles", "available_from", "sample_request", "sample_response", "druid_query")
          values ('druid-dataset', 'ml-task-detail-exhaust', '{}',
           'private', 'On-Demand', '1.0', '{"portal"}', '$submissionDate', '', '', '$druidQueryConf');""")

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', " +
      "'36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', " +
      "NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"

    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration

    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while(postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("requested_by") should be ("36b84ff5-2212-4219-bfed-24886969d890")
      postgresQuery.getString("requested_channel") should be ("ORG_001")
      postgresQuery.getString("err_message") should be ("")
      postgresQuery.getString("iteration") should be ("0")
      postgresQuery.getString("encryption_key") should be ("test@123")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-task-detail-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "insert status as Invalid request in the absence request_data" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(List())).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', NULL, '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', NULL, NULL, NULL, 0, 'Invalid request' ,1,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration

    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("Invalid request")
      postgresQuery.getString("download_urls") should be ("{}")
    }
  }

  it should "insert as failed with No Range" in {
    val query = DruidQueryModel("scan", "sl-project", "", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(List())).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-no-range\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format": "csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],
        |"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId ="888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
    }
  }

  it should "insert status as FAILED  with No Interval" in {
    val query = DruidQueryModel("scan", "sl-project", "", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(List())).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-no-interval\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
      postgresQuery.getString("err_message") should be ("Invalid request")
    }
  }

  it should "insert status as Success with interval" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)

    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f",
            "area_of_improvement":"Education Leader","school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI",
            "program_name":"Prerak Head Teacher of the Block 19-20","state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"inProgress"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-static-interval\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-task-detail-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "insert status as SUCCESS encryption key not provided" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","status_of_project","tasks","sub_task","task_evidence","task_remarks")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f","area_of_improvement":"Education Leader",
            "school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI","program_name":"Prerak Head Teacher of the Block 19-20",
            "state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"completed"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{ml_reports/ml-task-detail-exhaust/1626335633616_888700F9A860E7A42DA968FBECDF3F22.csv}', NULL, NULL, 0, '' " +
      ",0,NULL);")

    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='ml-task-detail-exhaust'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("err_message") should be ("")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-task-detail-exhaust/"+requestId+"_"+reportDate+".csv}")
    }
  }

  it should "execute the update and save request method" in {
    val jobRequest = JobRequest("126796199493140000", "888700F9A860E7A42DA968FBECDF3F22", "druid-dataset", "SUBMITTED", "{\"type\": \"ml-task-detail-exhaust\"," +
      "\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}",
      "36b84ff5-2212-4219-bfed-24886969d890", "ORG_001", System.currentTimeMillis(), None, None, None, Option(0), Option("") ,Option(0),Option("test@123"))
    val req = new JobRequest()
    val jobRequestArr = Array(jobRequest)
    val storageConfig = StorageConfig("local", "", outputLocation)
    implicit val conf = spark.sparkContext.hadoopConfiguration

    OnDemandDruidExhaustJob.saveRequests(storageConfig, jobRequestArr)
  }

  it should "generate the report with quote column" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f","area_of_improvement":"Education Leader",
            "school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI","program_name":"Prerak Head Teacher of the Block 19-20",
            "state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"inProgress"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-quote-column\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', " +
      "'{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{ "type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |        "key":"ml_reports/","format": "csv","quoteColumns": ["Role","Declared State","District","Block","School Name","Organisation Name",
        |        "Program Name","Project Title","Project Objective","Category","Tasks","Sub-Tasks","Remarks"]},"output":[{"to":"file",
        |        "params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId ="888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-task-detail-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate the report  with no label" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f","area_of_improvement":"Education Leader",
            "school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI","program_name":"Prerak Head Teacher of the Block 19-20",
            "state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"inProgress"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000'," +
      " '888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-no-label\",\"params\":" +
      "{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', " +
      "'36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("FAILED")
    }
  }

  it should "insert status as failed when filter doesn't match" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f1"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","status_of_project","tasks","sub_task","task_evidence","task_remarks")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f",
            "area_of_improvement":"Education Leader","school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI",
            "program_name":"Prerak Head Teacher of the Block 19-20","state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"completed"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}]}}', " +
      "'36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-05-09 19:35:18.666', '{}', " +
      "NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"

    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration

    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
  }

  it should "generate report with other generic query" in {
    val query = DruidQueryModel("scan", "sl-observation", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","isAPrivateProgram",Option("false"),None),
        DruidFilter("equals","solution_type",Option("observation_with_out_rubric"),None),
        DruidFilter("equals","programId",Option("60549338acf1c71f0b2409c3"),None),
        DruidFilter("equals","solutionId",Option("605c934eda9dea6400302afc"),None))),None, None,
      Option(List("__time","createdBy","role_title","user_stateName","user_districtName","user_blockName","user_schoolUDISE_code","user_schoolName",
        "organisation_name","programName","programExternalId","solutionName","solutionExternalId","observationSubmissionId","questionExternalId","questionName",
        "questionResponseLabel","minScore","evidences","remarks")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String ="""
                        |{"questionName":"Tick the following which are available:","user_districtName":"ANANTAPUR","evidences":"<NULL>",
                        |"questionResponseLabel":"Newspaper Stands","solutionExternalId":"96e4f796-8d6c-11eb-abd8-441ca8998ea1-OBSERVATION-TEMPLATE_CHILD_V2",
                        |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,"minScore":"<NULL>","programName":"3.8.0 testing program",
                        |"date":"2021-07-09","questionExternalId":"P47_1616678305996-1616679757967","organisation_name":"Staging Custodian Organization",
                        |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea5191a","remarks":"<NULL>","user_blockName":"AGALI",
                        |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI","programExternalId":"PGM-3542-3.8.0_testing_program",
                        |"user_stateName":"Andhra Pradesh","observationSubmissionId":"60e848e9f1252714cff1c1a4"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);

    val json1: String ="""
                         |{"questionName":"Tick the following which are available:","user_districtName":"ANANTAPUR",
                         |"evidences":"<NULL>","questionResponseLabel":"Library books shelf/rack","solutionExternalId":
                         |"96e4f796-8d6c-11eb-abd8-441ca8998ea1-OBSERVATION-TEMPLATE_CHILD_V2",
                         |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,
                         |"minScore":"<NULL>","programName":"3.8.0 testing program","date":"2021-07-09",
                         |"questionExternalId":"P47_1616678305996-1616679757967",
                         |"organisation_name":"Staging Custodian Organization",
                         |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea5191a","remarks":"<NULL>",
                         |"user_blockName":"AGALI",
                         |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI",
                         |"programExternalId":"PGM-3542-3.8.0_testing_program","user_stateName":"Andhra Pradesh",
                         |"observationSubmissionId":"60e848e9f1252714cff1c1a4"}
                       """.stripMargin
    val doc1: Json = parse(json1).getOrElse(Json.Null);

    val json2: String = """{"questionName":"No of toilets in the school","user_districtName":"ANANTAPUR",
                          |"evidences":"<NULL>","questionResponseLabel":"2","solutionExternalId":
                          |"96e4f796-8d6c-11eb-abd8-441ca8998ea1-OBSERVATION-TEMPLATE_CHILD_V2",
                          |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,
                          |"minScore":"<NULL>","programName":"3.8.0 testing program","date":"2021-07-09",
                          |"questionExternalId":"P1_1616678305996-1616679757967",
                          |"organisation_name":"Staging Custodian Organization",
                          |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea5191a","remarks":"<NULL>",
                          |"user_blockName":"AGALI",
                          |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI",
                          |"programExternalId":"PGM-3542-3.8.0_testing_program","user_stateName":"Andhra Pradesh",
                          |"observationSubmissionId":"60e848e9f1252714cff1c1a4"}""".stripMargin
    val doc2: Json = parse(json2).getOrElse(Json.Null);

    val json3: String = """{"questionName":"No of boys in the school","user_districtName":"ANANTAPUR",
                          |"evidences":"<NULL>","questionResponseLabel":"50","solutionExternalId":
                          |"96e4f796-8d6c-11eb-abd8-441ca8998ea1-OBSERVATION-TEMPLATE_CHILD_V2",
                          |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,
                          |"minScore":"<NULL>","programName":"3.8.0 testing program","date":"2021-07-09",
                          |"questionExternalId":"P1_1616678305996-1616679757967",
                          |"organisation_name":"Staging Custodian Organization",
                          |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea5191a","remarks":"<NULL>",
                          |"user_blockName":"AGALI",
                          |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI",
                          |"programExternalId":"PGM-3542-3.8.0_testing_program","user_stateName":"Andhra Pradesh",
                          |"observationSubmissionId":"60e848e9f1252714cff1c1a4"}""".stripMargin
    val doc3: Json = parse(json3).getOrElse(Json.Null);

    val json4: String = """{"questionName":"No of girls in the school","user_districtName":"ANANTAPUR",
                          |"evidences":"<NULL>","questionResponseLabel":"100","solutionExternalId":
                          |"96e4f796-8d6c-11eb-abd8-441ca8998ea1-OBSERVATION-TEMPLATE_CHILD_V2",
                          |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,
                          |"minScore":"<NULL>","programName":"3.8.0 testing program","date":"2021-07-09",
                          |"questionExternalId":"P3_1616678305996-1616679757967",
                          |"organisation_name":"Staging Custodian Organization",
                          |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea5191a","remarks":"<NULL>",
                          |"user_blockName":"AGALI",
                          |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI",
                          |"programExternalId":"PGM-3542-3.8.0_testing_program","user_stateName":"Andhra Pradesh",
                          |"observationSubmissionId":"60e848e9f1252714cff1c1a4"}""".stripMargin
    val doc4: Json = parse(json4).getOrElse(Json.Null)

    val json5: String = """{"questionName":"No of toilets in the school","user_districtName":"ANANTAPUR","evidences":"<NULL>",
                          |"questionResponseLabel":"1","solutionExternalId":"96e4f796-8d6c-11eb-abd8-441ca9966jhgj-OBSERVATION-TEMPLATE_CHILD_V2",
                          |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,"minScore":"<NULL>","programName":"3.8.0 testing program",
                          |"date":"2021-07-09","questionExternalId":"P3_1616678305996-1616679757968","organisation_name":"Staging Custodian Organization",
                          |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea123sd","remarks":"<NULL>","user_blockName":"AGALI",
                          |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI","programExternalId":"PGM-3542-4.0.0_testing_program",
                          |"user_stateName":"Andhra Pradesh","observationSubmissionId":"60e848e9f1252714cff1c1a5"}""".stripMargin
    val doc5: Json = parse(json5).getOrElse(Json.Null)

    val json6: String = """{"questionName":"Tick the following which are available:","user_districtName":"ANANTAPUR","evidences":"<NULL>",
                          |"questionResponseLabel":"Newspaper Stands","solutionExternalId":"96e4f796-8d6c-11eb-abd8-441ca9966jhgj-OBSERVATION-TEMPLATE_CHILD_V2",
                          |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,"minScore":"<NULL>","programName":"3.8.0 testing program",
                          |"date":"2021-07-09","questionExternalId":"P47_1616678305996-1616679757968","organisation_name":"Staging Custodian Organization",
                          |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea123sd","remarks":"<NULL>","user_blockName":"AGALI",
                          |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI","programExternalId":"PGM-3542-4.0.0_testing_program",
                          |"user_stateName":"Andhra Pradesh","observationSubmissionId":"60e848e9f1252714cff1c1a5"}""".stripMargin
    val doc6: Json = parse(json6).getOrElse(Json.Null)

    val json7: String = """{"questionName":"No of girls in the school","user_districtName":"ANANTAPUR","evidences":"<NULL>",
                          |"questionResponseLabel":"10","solutionExternalId":"96e4f796-8d6c-11eb-abd8-441ca9966jhgj-OBSERVATION-TEMPLATE_CHILD_V2",
                          |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,"minScore":"<NULL>","programName":"3.8.0 testing program",
                          |"date":"2021-07-09","questionExternalId":"P1_1616678305996-1616679757968","organisation_name":"Staging Custodian Organization",
                          |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea123sd","remarks":"<NULL>","user_blockName":"AGALI",
                          |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI","programExternalId":"PGM-3542-4.0.0_testing_program",
                          |"user_stateName":"Andhra Pradesh","observationSubmissionId":"60e848e9f1252714cff1c1a5"}""".stripMargin
    val doc7: Json = parse(json7).getOrElse(Json.Null)

    val json8: String = """{"questionName":"No of boys in the school","user_districtName":"ANANTAPUR","evidences":"<NULL>",
                          |"questionResponseLabel":"5","solutionExternalId":"96e4f796-8d6c-11eb-abd8-441ca9966jhgj-OBSERVATION-TEMPLATE_CHILD_V2",
                          |"user_schoolUDISE_code":"28226200815","role_title":"hm","__time":1.6258464E12,"minScore":"<NULL>","programName":"3.8.0 testing program",
                          |"date":"2021-07-09","questionExternalId":"P2_1616678305996-1616679757968","organisation_name":"Staging Custodian Organization",
                          |"createdBy":"7a8fa12b-75a7-41c5-9180-538f5ea123sd","remarks":"<NULL>","user_blockName":"AGALI",
                          |"solutionName":"School Needs Assessment - Primary","user_schoolName":"APMS AGALI","programExternalId":"PGM-3542-4.0.0_testing_program",
                          |"user_stateName":"Andhra Pradesh","observationSubmissionId":"60e848e9f1252714cff1c1a5"}""".stripMargin
    val doc8: Json = parse(json8).getOrElse(Json.Null)

    val events = List(DruidScanResult.apply(doc),DruidScanResult.apply(doc5),DruidScanResult.apply(doc1),
      DruidScanResult.apply(doc6),DruidScanResult.apply(doc2),DruidScanResult.apply(doc7),
      DruidScanResult.apply(doc3),DruidScanResult.apply(doc4),DruidScanResult.apply(doc8))
    val results = DruidScanResults.apply("sl-observation_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
          "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
          "'999700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-obs-question-detail-exhaust\",\"params\":" +
          "{\"filters\":[{\"type\":\"equals\",\"dimension\":\"isAPrivateProgram\",\"value\":\"false\"}," +
          "{\"type\":\"equals\",\"dimension\":\"solution_type\",\"value\":\"observation_with_out_rubric\"},{\"type\":\"equals\",\"dimension\":\"programId\",\"value\":\"60549338acf1c71f0b2409c3\"}," +
          "{\"type\":\"equals\",\"dimension\":\"solutionId\",\"value\":\"605c934eda9dea6400302afc\"}]}}', " +
          "'36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', '2021-07-15 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'demo@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "999700F9A860E7A42DA968FBECDF3F22"

    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while(postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("requested_by") should be ("36b84ff5-2212-4219-bfed-24886969d890")
      postgresQuery.getString("requested_channel") should be ("ORG_001")
      postgresQuery.getString("err_message") should be ("")
      postgresQuery.getString("iteration") should be ("0")
      postgresQuery.getString("encryption_key") should be ("demo@123")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-obs-question-detail-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate report with all the 4 filters(district,organisation,program,solution)" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None),
        DruidFilter("equals","district_externalId",Option("8250d58d-f1a2-4397-bfd3-b2e688ba7141"),None),
        DruidFilter("equals","organisation_id",Option("0126796199493140480"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)

    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f",
            "area_of_improvement":"Education Leader","school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI",
            "program_name":"Prerak Head Teacher of the Block 19-20","state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"inProgress"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-static-interval\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}," +
      "{\"type\":\"equals\",\"dimension\":\"district_externalId\",\"value\":\"8250d58d-f1a2-4397-bfd3-b2e688ba7141\"}," +
      "{\"type\":\"equals\",\"dimension\":\"organisation_id\",\"value\":\"0126796199493140480\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-task-detail-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate report with all the 3 filters(program,solution,district)" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None),
        DruidFilter("equals","district_externalId",Option("8250d58d-f1a2-4397-bfd3-b2e688ba7141"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)

    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f",
            "area_of_improvement":"Education Leader","school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI",
            "program_name":"Prerak Head Teacher of the Block 19-20","state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"inProgress"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-static-interval\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}," +
      "{\"type\":\"equals\",\"dimension\":\"district_externalId\",\"value\":\"8250d58d-f1a2-4397-bfd3-b2e688ba7141\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-task-detail-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate report with all the 3 filters(program,solution,organisation)" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("602512d8e6aefa27d9629bc3"),None),
        DruidFilter("equals","solution_id",Option("602a19d840d02028f3af00f0"),None),
        DruidFilter("equals","organisation_id",Option("0126796199493140480"),None))),None, None,
      Option(List("__time","createdBy","designation","state_name","district_name","block_name",
        "school_name","school_externalId", "organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description", "area_of_improvement",
        "project_duration","tasks","sub_task","task_evidence","task_remarks","status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)

    val json: String ="""
            {"block_name":"ALLAVARAM","project_title_editable":"Test-कृपया उस प्रोजेक्ट का शीर्षक जोड़ें जिसे आप ब्लॉक के Prerak HT के लिए सबमिट करना चाहते हैं","task_evidence":"<NULL>",
            "designation":"hm","school_externalId":"unknown","project_duration":"1 महीना","__time":1.6133724E12,"sub_task":"<NULL>",
            "tasks":"यहां, आप अपने विद्यालय में परियोजना को पूरा करने के लिए अपने द्वारा किए गए कार्यों ( Tasks)को जोड़ सकते हैं।","project_id":"602a19d840d02028f3af00f0",
            "project_description":"test","program_externalId":"PGM-Prerak-Head-Teacher-of-the-Block-19-20-Feb2021",
            "organisation_name":"Pre-prod Custodian Organization","createdBy":"7651c7ab-88f9-4b23-8c1d-ac8d92844f8f",
            "area_of_improvement":"Education Leader","school_name":"MPPS (GN) SAMANTHAKURRU","district_name":"EAST GODAVARI",
            "program_name":"Prerak Head Teacher of the Block 19-20","state_name":"Andhra Pradesh","task_remarks":"<NULL>","status_of_project":"inProgress"}
             """.stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F22', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-static-interval\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"602512d8e6aefa27d9629bc3\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"602a19d840d02028f3af00f0\"}," +
      "{\"type\":\"equals\",\"dimension\":\"organisation_id\",\"value\":\"0126796199493140480\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{}', NULL, NULL, 0, '' ,0,'test@123');")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-task-detail-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate project status csv with program dashboard date range filter with both start_date and end_date" in {
    val query = DruidQueryModel("scan", "sl-project", "2022-06-01T05:30:00/2022-07-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("62034f90841a270008e82e46"),None),
        DruidFilter("equals","solution_id",Option("6209fcfa841a270008e84603"),None)
      )),None, None,
      Option(List("__time","createdBy","user_type","designation","state_name","district_name","block_name",
        "school_name","school_externalId","board_name","organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description",
        "project_created_date","project_completed_date","project_duration","project_last_sync",
        "status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String =
      """
        |{"user_type":"<NULL>","organisation_name":"<NULL>","project_last_sync":"2022-06-25T14:12:00.519+05:30",
        |"project_duration":"2 weeks","project_created_date":"2022-06-25T12:54:20.758+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"ecd5bbb9-5db2-4720-a51c-12fe1715d406","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-25T14:12:00.528+05:30","school_externalId":28162600904,
        |"district_name":"KRISHNA","project_id":"62b7057c7871b60008d35b22",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260","designation":"HM",
        |"state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"BAPULAPADU","__time":"1656146520528","school_name":"ZPHS REMALLI",
        |"board_name":"<NULL>"}""".stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val json1: String =
      """
        |{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"2022-06-23T15:36:05.301+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-23T15:32:48.599+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"154ab19a-e91d-414d-840c-159f7378949e","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-23T15:36:05.311+05:30","school_externalId":28144200402,
        |"district_name":"EAST GODAVARI","project_id":"62b487a07871b60008d3590c",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"HM,DEO","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260","block_name":"ALAMURU",
        |"__time":"1655978765311","school_name":"MPPS NARSIPUDI COLONY",
        |"board_name":"<NULL>"}""".stripMargin
    val doc1: Json = parse(json1).getOrElse(Json.Null);
    val json2: String =
      """{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"2022-06-27T13:28:37.579+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-27T13:25:46.659+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"2b7c9de2-f930-4f3b-8bdb-f1a1e583f309","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-27T13:28:37.588+05:30","school_externalId":"<NULL>",
        |"district_name":"KRISHNA","project_id":"62b9afda7871b60008d35eae",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"DEO","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"A.KONDURU","__time":"1656316717588","school_name":"<NULL>",
        |"board_name":"<NULL>"}""".stripMargin
    val doc2: Json = parse(json2).getOrElse(Json.Null);
    val json3: String =
      """{"user_type":"administrator",
        |"organisation_name":"Staging Custodian Organization",
        |"project_last_sync":"2022-06-29T11:50:59.497+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-29T11:48:49.477+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"ef0325e4-db5b-4dfc-95a8-89b3e53aca0e","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-29T11:50:59.512+05:30","school_externalId":28226200404,
        |"district_name":"ANANTAPUR","project_id":"62bc3c24cf8b5d0008906e5a",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"HM,DEO","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260","block_name":"AGALI",
        |"__time":"1656483659512","school_name":"MPPS RAMANAHALLI",
        |"board_name":"State (Karnataka)"}""".stripMargin
    val doc3: Json = parse(json3).getOrElse(Json.Null);
    val json4: String =
      """{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"2022-06-23T09:45:22.956+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-23T08:15:34.440+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"3b1b8620-c22d-47ce-9fed-35adb074d7ee","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-23T09:45:22.967+05:30","school_externalId":28144200303,
        |"district_name":"EAST GODAVARI","project_id":"62b421267871b60008d357bb",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"DEO,HM","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"ALAMURU","__time":"1655957722967",
        |"school_name":"MPPS (GN) CHEMUDULANKA","board_name":"<NULL>"}""".stripMargin
    val doc4: Json = parse(json4).getOrElse(Json.Null)
    val json5: String =
      """{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"<NULL>","project_duration":"2 weeks",
        |"project_created_date":"2022-06-03T06:28:21.954+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"started",
        |"createdBy":"fe7ecff7-3086-489d-9ac2-02634e641d12","program_name":"Testing program 4.7",
        |"project_completed_date":"<NULL>","school_externalId":28192590424,"district_name":"NELLORE",
        |"project_id":"6299aa057871b60008d3411c",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"HM,DEO,SPD","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"NELLORE","__time":"1654217901977",
        |"school_name":"MCPS SANTHAPET","board_name":"<NULL>"}""".stripMargin
    val doc5: Json = parse(json5).getOrElse(Json.Null)
    val json6: String =
      """{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"2022-06-16T10:58:19.377+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-15T07:34:50.603+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"inProgress",
        |"createdBy":"0e0ebece-85c1-4855-b83d-666df314fa92","program_name":"Testing program 4.7",
        |"project_completed_date":"<NULL>","school_externalId":28162102201,"district_name":"KRISHNA",
        |"project_id":"62a98b9a7871b60008d34eb2",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"HM,DEO,SPD","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"GANNAVARAM","__time":"1655357299389",
        |"school_name":"MPPS ALLAPURAM","board_name":"<NULL>"}""".stripMargin
    val doc6: Json = parse(json6).getOrElse(Json.Null)
    val events = List(DruidScanResult.apply(doc),DruidScanResult.apply(doc5),
      DruidScanResult.apply(doc1), DruidScanResult.apply(doc6),DruidScanResult.apply(doc2),
      DruidScanResult.apply(doc3),DruidScanResult.apply(doc4))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140123', " +
      "'852ECFC2A5C74B6071727A70F247A3D6', 'druid-dataset', 'SUBMITTED', " +
      "'{\"type\":\"ml-project-status-exhaust\",\"params\":{\"start_date\":\"2022-06-01\"," +
      "\"end_date\":\"2022-07-01\",\"filters\":[{\"type\":\"equals\",\"dimension\":" +
      "\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"sub_task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"project_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"program_id\",\"value\":\"62034f90841a270008e82e46\"},{\"type\":\"equals\",\"dimension\":" +
      "\"solution_id\",\"value\":\"6209fcfa841a270008e84603\"}]},\"title\":\"Status Report\"}', " +
      "'3b200146-5c0c-4e95-ae06-dacb89460d99', 'ORG_001', '2022-07-08 13:20:18.666', '{}', NULL, NULL, 0, '' ,0,NULL);")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "852ECFC2A5C74B6071727A70F247A3D6"

    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while(postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("requested_by") should be ("3b200146-5c0c-4e95-ae06-dacb89460d99")
      postgresQuery.getString("requested_channel") should be ("ORG_001")
      postgresQuery.getString("err_message") should be ("")
      postgresQuery.getString("iteration") should be ("0")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-project-status-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate project status csv with program dashboard date range filter with no start_date and no end_date" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("62034f90841a270008e82e46"),None),
        DruidFilter("equals","solution_id",Option("6209fcfa841a270008e84603"),None),
        DruidFilter("equals","district_externalId",Option("2f76dcf5-e43b-4f71-a3f2-c8f19e1fce03"),None)
      )),None, None,
      Option(List("__time","createdBy","user_type","designation","state_name","district_name","block_name",
        "school_name","school_externalId","board_name","organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description",
        "project_created_date","project_completed_date","project_duration","project_last_sync",
        "status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String =
      """
        |{"user_type":"<NULL>","organisation_name":"<NULL>","project_last_sync":"2022-06-25T14:12:00.519+05:30",
        |"project_duration":"2 weeks","project_created_date":"2022-06-25T12:54:20.758+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"ecd5bbb9-5db2-4720-a51c-12fe1715d406","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-25T14:12:00.528+05:30","school_externalId":28162600904,
        |"district_name":"KRISHNA","project_id":"62b7057c7871b60008d35b22",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260","designation":"HM",
        |"state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"BAPULAPADU","__time":"1656146520528","school_name":"ZPHS REMALLI",
        |"board_name":"<NULL>"}""".stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val json1: String =
      """{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"2022-06-27T13:28:37.579+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-27T13:25:46.659+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"2b7c9de2-f930-4f3b-8bdb-f1a1e583f309","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-27T13:28:37.588+05:30","school_externalId":"<NULL>",
        |"district_name":"KRISHNA","project_id":"62b9afda7871b60008d35eae",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"DEO","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"A.KONDURU","__time":"1656316717588","school_name":"<NULL>",
        |"board_name":"<NULL>"}""".stripMargin
    val doc1: Json = parse(json1).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc),DruidScanResult.apply(doc1))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140123', " +
      "'852ECFC2A5C74B6071727A70F247A3D6', 'druid-dataset', 'SUBMITTED', " +
      "'{\"type\":\"ml-project-status-exhaust\",\"params\":{\"filters\":[{\"type\":\"equals\",\"dimension\":" +
      "\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"sub_task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"project_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"program_id\",\"value\":\"62034f90841a270008e82e46\"},{\"type\":\"equals\",\"dimension\":" +
      "\"solution_id\",\"value\":\"6209fcfa841a270008e84603\"}," +
      "{\"type\":\"equals\",\"dimension\":\"district_externalId\",\"value\":\"2f76dcf5-e43b-4f71-a3f2-c8f19e1fce03\"}]},\"title\":\"Status Report\"}', " +
      "'3b200146-5c0c-4e95-ae06-dacb89460d99', 'ORG_001', '2022-07-08 13:20:18.666', '{}', NULL, NULL, 0, '' ,0,NULL);")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "852ECFC2A5C74B6071727A70F247A3D6"

    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while(postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("requested_by") should be ("3b200146-5c0c-4e95-ae06-dacb89460d99")
      postgresQuery.getString("requested_channel") should be ("ORG_001")
      postgresQuery.getString("err_message") should be ("")
      postgresQuery.getString("iteration") should be ("0")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-project-status-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate project status csv with program dashboard date range filter with start_date and no end_date" in {
    val query = DruidQueryModel("scan", "sl-project", "2022-06-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("62034f90841a270008e82e46"),None),
        DruidFilter("equals","solution_id",Option("6209fcfa841a270008e84603"),None),
        DruidFilter("equals","district_externalId",Option("2f76dcf5-e43b-4f71-a3f2-c8f19e1fce03"),None),
        DruidFilter("equals","organisation_id",Option("0126796199493140480"),None)
      )),None, None,
      Option(List("__time","createdBy","user_type","designation","state_name","district_name","block_name",
        "school_name","school_externalId","board_name","organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description",
        "project_created_date","project_completed_date","project_duration","project_last_sync",
        "status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String =
      """{"user_type":"administrator",
        |"organisation_name":"Staging Custodian Organization",
        |"project_last_sync":"2022-06-29T11:50:59.497+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-29T11:48:49.477+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"submitted",
        |"createdBy":"ef0325e4-db5b-4dfc-95a8-89b3e53aca0e","program_name":"Testing program 4.7",
        |"project_completed_date":"2022-06-29T11:50:59.512+05:30","school_externalId":28226200404,
        |"district_name":"ANANTAPUR","project_id":"62bc3c24cf8b5d0008906e5a",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"HM,DEO","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260","block_name":"AGALI",
        |"__time":"1656483659512","school_name":"MPPS RAMANAHALLI",
        |"board_name":"State (Karnataka)"}""".stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140123', " +
      "'852ECFC2A5C74B6071727A70F247A3D6', 'druid-dataset', 'SUBMITTED', " +
      "'{\"type\":\"ml-project-status-exhaust\",\"params\":{\"start_date\":\"2022-06-01\"," +
      "\"filters\":[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":" +
      "\"sub_task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"project_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"program_id\",\"value\":\"62034f90841a270008e82e46\"},{\"type\":\"equals\",\"dimension\":" +
      "\"solution_id\",\"value\":\"6209fcfa841a270008e84603\"}," +
      "{\"type\":\"equals\",\"dimension\":\"district_externalId\",\"value\":\"2f76dcf5-e43b-4f71-a3f2-c8f19e1fce03\"}," +
      "{\"type\":\"equals\",\"dimension\":\"organisation_id\",\"value\":\"0126796199493140480\"}]},\"title\":\"Status Report\"}', " +
      "'3b200146-5c0c-4e95-ae06-dacb89460d99', 'ORG_001', '2022-07-08 13:20:18.666', '{}', NULL, NULL, 0, '' ,0,NULL);")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "852ECFC2A5C74B6071727A70F247A3D6"

    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while(postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("requested_by") should be ("3b200146-5c0c-4e95-ae06-dacb89460d99")
      postgresQuery.getString("requested_channel") should be ("ORG_001")
      postgresQuery.getString("err_message") should be ("")
      postgresQuery.getString("iteration") should be ("0")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-project-status-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate project status csv with program dashboard date range filter with end_date and no start_date" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2022-07-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals","private_program",Option("false"),None),
        DruidFilter("equals","sub_task_deleted_flag",Option("false"),None),
        DruidFilter("equals","task_deleted_flag",Option("false"),None),
        DruidFilter("equals","project_deleted_flag",Option("false"),None),
        DruidFilter("equals","program_id",Option("62034f90841a270008e82e46"),None),
        DruidFilter("equals","solution_id",Option("6209fcfa841a270008e84603"),None)
      )),None, None,
      Option(List("__time","createdBy","user_type","designation","state_name","district_name","block_name",
        "school_name","school_externalId","board_name","organisation_name","program_name",
        "program_externalId","project_id","project_title_editable","project_description",
        "project_created_date","project_completed_date","project_duration","project_last_sync",
        "status_of_project")), None, None,None,None,None,0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String =
      """{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"<NULL>","project_duration":"2 weeks",
        |"project_created_date":"2022-06-03T06:28:21.954+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"started",
        |"createdBy":"fe7ecff7-3086-489d-9ac2-02634e641d12","program_name":"Testing program 4.7",
        |"project_completed_date":"<NULL>","school_externalId":28192590424,"district_name":"NELLORE",
        |"project_id":"6299aa057871b60008d3411c",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"HM,DEO,SPD","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"NELLORE","__time":"1654217901977",
        |"school_name":"MCPS SANTHAPET","board_name":"<NULL>"}""".stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null)
    val json1: String =
      """{"user_type":"<NULL>","organisation_name":"<NULL>",
        |"project_last_sync":"2022-06-16T10:58:19.377+05:30","project_duration":"2 weeks",
        |"project_created_date":"2022-06-15T07:34:50.603+05:30",
        |"program_externalId":"PGM-FD255-testing_program-4.7","status_of_project":"inProgress",
        |"createdBy":"0e0ebece-85c1-4855-b83d-666df314fa92","program_name":"Testing program 4.7",
        |"project_completed_date":"<NULL>","school_externalId":28162102201,"district_name":"KRISHNA",
        |"project_id":"62a98b9a7871b60008d34eb2",
        |"project_title_editable":"Project with Mandatory Tasks and Subtasks - FD260",
        |"designation":"HM,DEO,SPD","state_name":"Andhra Pradesh",
        |"project_description":"Project with Mandatory Tasks and Subtasks - FD260",
        |"block_name":"GANNAVARAM","__time":"1655357299389",
        |"school_name":"MPPS ALLAPURAM","board_name":"<NULL>"}""".stripMargin
    val doc1: Json = parse(json1).getOrElse(Json.Null)
    val events = List(DruidScanResult.apply(doc),DruidScanResult.apply(doc1))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45",List(),events)
    val druidResponse =  DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_:com.ing.wbaa.druid.DruidQuery)(_:DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140123', " +
      "'852ECFC2A5C74B6071727A70F247A3D6', 'druid-dataset', 'SUBMITTED', " +
      "'{\"type\":\"ml-project-status-exhaust\",\"params\":{"+
      "\"end_date\":\"2022-07-01\",\"filters\":[{\"type\":\"equals\",\"dimension\":" +
      "\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"sub_task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"task_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"project_deleted_flag\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":" +
      "\"program_id\",\"value\":\"62034f90841a270008e82e46\"},{\"type\":\"equals\",\"dimension\":" +
      "\"solution_id\",\"value\":\"6209fcfa841a270008e84603\"}]},\"title\":\"Status Report\"}', " +
      "'3b200146-5c0c-4e95-ae06-dacb89460d99', 'ORG_001', '2022-07-08 13:20:18.666', '{}', NULL, NULL, 0, '' ,0,NULL);")
    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin

    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "852ECFC2A5C74B6071727A70F247A3D6"

    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()
    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='druid-dataset'")
    while(postgresQuery.next()) {
      postgresQuery.getString("status") should be ("SUCCESS")
      postgresQuery.getString("requested_by") should be ("3b200146-5c0c-4e95-ae06-dacb89460d99")
      postgresQuery.getString("requested_channel") should be ("ORG_001")
      postgresQuery.getString("err_message") should be ("")
      postgresQuery.getString("iteration") should be ("0")
      postgresQuery.getString("download_urls") should be (s"{ml_reports/ml-project-status-exhaust/"+requestId+"_"+reportDate+".zip}")
    }
  }

  it should "generate task detail csv by creating absolute evidence path for task and project" in {
    val query = DruidQueryModel("scan", "sl-project", "1901-01-01T05:30:00/2101-01-01T05:30:00", Option("all"),
      None, None, Option(List(DruidFilter("equals", "private_program", Option("false"), None),
        DruidFilter("equals", "sub_task_deleted_flag", Option("false"), None),
        DruidFilter("equals", "task_deleted_flag", Option("false"), None),
        DruidFilter("equals", "project_deleted_flag", Option("false"), None),
        DruidFilter("equals", "program_id", Option("6172a6e58cf7b10007eefd21"), None),
        DruidFilter("equals", "solution_id", Option("6177ec9d65117d0007668b85"), None))), None, None,
      Option(List("__time", "createdBy", "user_type", "designation", "state_name", "district_name", "block_name",
        "school_name", "school_code", "board_name", "organisation_name", "program_name",
        "program_externalId", "project_id", "project_title_editable", "project_description", "area_of_improvement",
        "project_created_date", "project_completed_date", "project_duration", "status_of_project", "tasks", "sub_task",
        "task_evidence", "task_remarks", "project_evidence", "project_remarks")), None, None, None, None, None, 0)
    val druidQuery = DruidDataFetcher.getDruidQuery(query)
    val json: String ="""{"__time":"1613372400000","designation":"DEO,HM","project_created_date":"2022-11-10T10:03:57.007+05:30",
      |"program_externalId":"PGM_FD_98_TEST_4.4","user_type":"administrator","organisation_name":"Staging Custodian Organization",
      |"area_of_improvement":"'Education Leader, Teachers'","school_name":"MPPS KASAPURAM",
      |"project_completed_date":"2022-11-10T10:13:39.797+05:30","project_description":"' HM will be able to take the self-assessment for their school'",
      |"project_evidence":"survey/636ccc8da5db5c0008d50350/bf1b621c-d039-47b7-9f11-658270b1f9f9/e46a59da-7fef-4f75-9c5a-7daf26b3c9e9/1668075205969.pdf",
      |"program_name":"Testing 4.4","block_name":"AGALI","project_duration":"2 weeks","district_name":"ANANTAPUR",
      |"project_id":"636ccc8da5db5c0008d50350","createdBy":"bf1b621c-d039-47b7-9f11-658270b1f9f9","sub_task":"<NULL>","board_name":"CBSE",
      |"school_code":28226200402,"project_title_editable":"' Project link consumption -FD 98'",
      |"task_evidence":"survey/636ccc8da5db5c0008d50350/bf1b621c-d039-47b7-9f11-658270b1f9f9/e46a59da-7fef-4f75-9c5a-7daf26b3c9e9/1668075089411.jpg",
      |"task_remarks":"'ok ok ok'","project_remarks":"'no no no'","tasks":"'Create an action plan with timelines and process for review'",
      |"state_name":"Andhra Pradesh","status_of_project":"submitted"}""".stripMargin
    val doc: Json = parse(json).getOrElse(Json.Null);
    val json2: String =
      """{"__time":"1613372400000","designation":"DEO,HM","project_created_date":"2022-11-10T10:03:57.007+05:30",
        |"program_externalId":"PGM_FD_98_TEST_4.4","user_type":"administrator","organisation_name":"Staging Custodian Organization",
        |"area_of_improvement":"'Education Leader, Teachers'","school_name":"MPPS KASAPURAM",
        |"project_completed_date":"2022-11-10T10:13:39.797+05:30","project_description":"' HM will be able to take the self-assessment for their school'",
        |"project_evidence":"samiksha/survey/636ccc8da5db5c0008d50350/bf1b621c-d039-47b7-9f11-658270b1f9f9/e46a59da-7fef-4f75-9c5a-7daf26b3c9e9/1668075211903.jpg",
        |"program_name":"Testing 4.4","block_name":"AGALI","project_duration":"2 weeks","district_name":"ANANTAPUR","project_id":"636ccc8da5db5c0008d50350",
        |"createdBy":"bf1b621c-d039-47b7-9f11-658270b1f9f9","sub_task":"<NULL>","board_name":"CBSE","school_code":28226200402,
        |"project_title_editable":"' Project link consumption -FD 98'","task_evidence":"survey/636ccc8da5db5c0008d50350/bf1b621c-d039-47b7-9f11-658270b1f9f9/e46a59da-7fef-4f75-9c5a-7daf26b3c9e9/1668075118304.pdf",
        |"task_remarks":"<NULL>","project_remarks":"<NULL>","tasks":"'Create an action plan with timelines and process for review'",
        |"state_name":"Andhra Pradesh","status_of_project":"submitted"}""".stripMargin
    val doc2: Json = parse(json2).getOrElse(Json.Null);
    val events = List(DruidScanResult.apply(doc),DruidScanResult.apply(doc2))
    val results = DruidScanResults.apply("sl-project_2020-06-08T00:00:00.000Z_2020-06-09T00:00:00.000Z_2020-11-20T06:13:29.089Z_45", List(), events)
    val druidResponse = DruidScanResponse.apply(List(results))
    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQueryAsStream(_: com.ing.wbaa.druid.DruidQuery)(_: DruidConfig)).expects(druidQuery, mockDruidConfig)
      .returns(Source(events)).anyNumberOfTimes()
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_: String, _: String, _: String)).expects(*, *, *)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, " +
      "download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration, encryption_key) VALUES ('126796199493140000', " +
      "'888700F9A860E7A42DA968FBECDF3F39', 'druid-dataset', 'SUBMITTED', '{\"type\": \"ml-task-detail-exhaust-absolute-path\",\"params\":{\"filters\":" +
      "[{\"type\":\"equals\",\"dimension\":\"private_program\",\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"sub_task_deleted_flag\"," +
      "\"value\":\"false\"},{\"type\":\"equals\",\"dimension\":\"task_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"project_deleted_flag\",\"value\":\"false\"}," +
      "{\"type\":\"equals\",\"dimension\":\"program_id\",\"value\":\"6172a6e58cf7b10007eefd21\"}," +
      "{\"type\":\"equals\",\"dimension\":\"solution_id\",\"value\":\"6177ec9d65117d0007668b85\"}]}}', '36b84ff5-2212-4219-bfed-24886969d890', 'ORG_001', " +
      "'2021-05-09 19:35:18.666', '{ml_reports/ml-task-detail-exhaust-absolute-path/1626335633616_888700F9A860E7A42DA968FBECDF3F39.csv}', NULL, NULL, 0, '' " +
      ",0,NULL);")

    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val requestId = "888700F9A860E7A42DA968FBECDF3F22"
    implicit val config = jobConfig
    implicit val conf = spark.sparkContext.hadoopConfiguration
    OnDemandDruidExhaustJob.execute()

    val postgresQuery = EmbeddedPostgresql.executeQuery("SELECT * FROM job_request WHERE job_id='ml-task-detail-exhaust-absolute-path'")
    while (postgresQuery.next()) {
      postgresQuery.getString("status") should be("SUCCESS")
      postgresQuery.getString("err_message") should be("")
      postgresQuery.getString("download_urls") should be(s"{ml_reports/ml-task-detail-exhaust-absolute-path/" + requestId + "_" + reportDate + ".csv}")
    }
  }

  it should "execute main method" in {
    EmbeddedPostgresql.execute(s"TRUNCATE $jobRequestTable")
    EmbeddedPostgresql.execute("INSERT INTO job_request (tag, request_id, job_id, status, request_data, requested_by, requested_channel, dt_job_submitted, download_urls, dt_file_created, dt_job_completed, execution_time, err_message ,iteration) VALUES ('do_1131350140968632321230_batch-001:01250894314817126443', '37564CF8F134EE7532F125651B51D17F', 'response-exhaust', 'SUBMITTED', '{\"batchId\": \"batch-001\"}', 'user-002', 'b00bc992ef25f1a9a8d63291e20efc8d', '2020-10-19 05:58:18.666', '{}', NULL, NULL, 0, '' ,0);")

    val strConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"local","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    OnDemandDruidExhaustJob.main(strConfig)

  }

  // Coverage improvement
  it should "execute to cover exceptional methods" in {

    val s3Config =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"s3","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    OnDemandDruidExhaustJob.setReportsStorageConfiguration(JSONUtils.deserialize[JobConfig](s3Config))
    val azureConfig =
      """{"search":{"type":"none"},"model":"org.sunbird.analytics.exhaust.OnDemandDruidExhaustJob","modelParams":{"store":"azure","container":"test-container",
        |"key":"ml_reports/","format":"csv"},"output":[{"to":"file","params":{"file":"ml_reports/"}}],"parallelization":8,"appName":"ML Druid Data Model"}""".stripMargin
    OnDemandDruidExhaustJob.setReportsStorageConfiguration(JSONUtils.deserialize[JobConfig](azureConfig))

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (fc.getDruidClient: () => DruidClient).expects().returns(mockDruidClient).anyNumberOfTimes()
    (mockDruidClient.actorSystem _).expects().returning(ActorSystem("OnDemandDruidExhaustQuery")).anyNumberOfTimes()
    (fc.getHadoopFileUtil: () => HadoopFileUtil).expects()
      .returns(new HadoopFileUtil).anyNumberOfTimes()
    (fc.getStorageService(_:String,_:String,_:String)).expects(*,*,*)
      .returns(mock[BaseStorageService]).anyNumberOfTimes()
    (fc.getHadoopFileUtil _).expects().returns(hadoopFileUtil).anyNumberOfTimes();

    val jobRequest = JobRequest("126796199493140000", "888700F9A860E7A42DA968FBECDF3F22", "druid-dataset", "SUBMITTED", "{\"type\": \"ml-task-detail-exhaust\"," +
      "\"params\":{\"programId\" :\"602512d8e6aefa27d9629bc3\",\"solutionId\" : \"602a19d840d02028f3af00f0\"}}",
      "36b84ff5-2212-4219-bfed-24886969d890", "ORG_001", System.currentTimeMillis(), Option(List("")), None, None, Option(0), Option("") ,Option(0),Option("test@123"))
    val storageConfig = StorageConfig("local", "", outputLocation)
    implicit val conf = spark.sparkContext.hadoopConfiguration

    val updatedJobRequest = OnDemandDruidExhaustJob.processRequestEncryption(storageConfig, jobRequest)
    updatedJobRequest.download_urls.get should be(List(""))

    // coverage for canZipExceptionBeIgnored = false
    val updatedJobRequest1 = OnDemandDruidExhaustTestJob.processRequestEncryption(storageConfig, jobRequest)
    updatedJobRequest1.status should be ("FAILED")
    updatedJobRequest1.err_message.get should be("Zip, encrypt and upload failed")

    // coverage for zipEnabled = false
    val jobRequest2 = JobRequest("126796199493140000", "888700F9A860E7A42DA968FBECDF3F22", "druid-dataset", "SUBMITTED", "{\"type\": \"ml-task-detail-exhaust\"," +
      "\"params\":{\"programId\" :\"602512d8e6aefa27d9629bc3\",\"solutionId\" : \"602a19d840d02028f3af00f0\"}}",
      "36b84ff5-2212-4219-bfed-24886969d890", "ORG_001", System.currentTimeMillis(), Option(List("")), None, None, Option(0), Option("") ,Option(0),Option("test@123"))

    val updatedJobRequest2 = OnDemandDruidExhaustTestJob2.processRequestEncryption(storageConfig, jobRequest2)
    updatedJobRequest2.download_urls.get should be(List(""))
    updatedJobRequest2.status should be ("SUBMITTED")
  }
}

// Test object with canZipExceptionBeIgnored = false
object OnDemandDruidExhaustTestJob extends BaseReportsJob with Serializable with IJob with OnDemandBaseExhaustJob with BaseDruidQueryProcessor {
  implicit override val className: String = "org.sunbird.analytics.exhaust.OnDemandDruidExhaustTestJob"

  val jobId: String = "druid-dataset"
  val jobName: String = "OnDemandDruidExhaustTestJob"

  def name(): String = "OnDemandDruidExhaustTestJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val sc: SparkContext = spark.sparkContext
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)

    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val conf = spark.sparkContext.hadoopConfiguration
    try {
      val res = CommonUtil.time(execute());
      JobLogger.end(s"OnDemandDruidExhaustTestJob completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1)));
    } catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, ERROR);
    } finally {
      frameworkContext.closeContext();
      spark.close()
      cleanUp()
    }
  }

  override def canZipExceptionBeIgnored(): Boolean = false

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): String = {
    ""
  }
}

// Test object with zipEnabled = false
object OnDemandDruidExhaustTestJob2 extends BaseReportsJob with Serializable with IJob with OnDemandBaseExhaustJob with BaseDruidQueryProcessor {
  implicit override val className: String = "org.sunbird.analytics.exhaust.OnDemandDruidExhaustTestJob2"

  val jobId: String = "druid-dataset"
  val jobName: String = "OnDemandDruidExhaustTestJob2"

  def name(): String = "OnDemandDruidExhaustTestJob2"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val sc: SparkContext = spark.sparkContext
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)

    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val conf = spark.sparkContext.hadoopConfiguration
    try {
      val res = CommonUtil.time(execute());
      JobLogger.end(s"OnDemandDruidExhaustTestJob2 completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1)));
    } catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, ERROR);
    } finally {
      frameworkContext.closeContext();
      spark.close()
      cleanUp()
    }
  }

  override def zipEnabled(): Boolean = false

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): String = {
    ""
  }
}
