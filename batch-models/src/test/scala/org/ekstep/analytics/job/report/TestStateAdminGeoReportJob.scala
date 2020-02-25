package org.ekstep.analytics.job

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.{BaseReportsJob, StateAdminGeoReportJob}
import org.ekstep.analytics.util.EmbeddedCassandra
import org.sunbird.cloud.storage.conf.AppConf
import java.io.File

import org.ekstep.analytics.framework.{Fetcher, FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.HadoopFileUtil

class TestStateAdminGeoReportJob extends SparkSpec(null) with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var shadowUserDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql") // Load test data in embedded cassandra server
  }
  
  override def afterAll() : Unit = {
    super.afterAll();
    (new HadoopFileUtil()).delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/admin-user-reports")
  }

  "StateAdminGeoReportJob" should "generate reports" in {
    implicit val fc = new FrameworkContext()
    val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
    val reportDF = StateAdminGeoReportJob.generateGeoReport()(spark, fc)
    assert(reportDF.count() === 9)
    //for geo report we expect these columns
    assert(reportDF.columns.contains("index") === true)
    assert(reportDF.columns.contains("School id") === true)
    assert(reportDF.columns.contains("School name") === true)
    assert(reportDF.columns.contains("District id") === true)
    assert(reportDF.columns.contains("District name") === true)
    assert(reportDF.columns.contains("Block id") === true)
    assert(reportDF.columns.contains("Block name") === true)
    assert(reportDF.columns.contains("slug") === true)
    assert(reportDF.columns.contains("externalid") === true)
    val apslug = reportDF.where(col("slug") === "ApSlug")
    val school_name = apslug.select("School name").collect().map(_ (0)).toList
    assert(school_name(0) === "MPPS SIMHACHALNAGAR")
    assert(school_name(1) === "Another school")
    assert(reportDF.select("District id").distinct().count == 5)
    //checking reports were created under slug folder
    val geoDetail = new File("src/test/resources/admin-user-reports/geo-detail/ApSlug.csv")
    val geoSummary = new File("src/test/resources/admin-user-reports/geo-summary/ApSlug.json")
    val geoSummaryDistrict = new File("src/test/resources/admin-user-reports/geo-summary-district/ApSlug.json")
    assert(geoDetail.exists() === true)
    assert(geoSummary.exists() === true)
    assert(geoSummaryDistrict.exists() === true)
  }

  ignore should "execute dispatcher" in {
    implicit val fc = new FrameworkContext()
    val reportDF = StateAdminGeoReportJob.generateGeoReport()(spark, fc)

    val modelParams = Map[String, AnyRef]("adhoc_scripts_virtualenv_dir" -> "/Users/kumar/projects/sunbird-data-products/venv",
      "adhoc_scripts_output_dir" -> "/Users/kumar/projects/sunbird-data-products/reports")
    val jobConfig = JobConfig(Fetcher("local", None, None), None, None, "StateAdminJob", Some(modelParams), None, Some(4), Some("TestExecuteDispatchder"))
    StateAdminGeoReportJob.generateDistrictZip(reportDF, jobConfig)
  }
}