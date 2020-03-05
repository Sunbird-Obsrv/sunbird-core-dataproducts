package org.ekstep.analytics.job

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.job.report.{BaseReportSpec, CourseMetricsJob, ReportGenerator}
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.framework.StorageConfig
import org.ekstep.analytics.framework.util.HadoopFileUtil
import org.ekstep.analytics.util.EmbeddedES

import scala.collection.mutable

class TestCourseMetricsJob extends BaseReportSpec with MockFactory {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var externalIdentityDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {

    super.beforeAll()
    spark = getSparkSession();

    /*
     * Data created with 31 active batch from batchid = 1000 - 1031
     * */
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/courseBatchTable.csv")
      .cache()

    externalIdentityDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/usr_external_identity.csv")
      .cache()

    /*
     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
     * */
    userCoursesDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userCoursesTable.csv")
      .cache()

    /*
     * This has users 30 from user001 - user030
     * */
    userDF = spark
      .read
      .json("src/test/resources/course-metrics-updater/userTable.json")
      .cache()

    /*
     * This has 30 unique location
     * */
    locationDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/locationTable.csv")
      .cache()

    /*
     * There are 8 organisation added to the data, which can be mapped to `rootOrgId` in user table
     * and `organisationId` in userOrg table
     * */
    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/orgTable.csv")
      .cache()

    /*
     * Each user is mapped to organisation table from any of 8 organisation
     * */
    userOrgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userOrgtable.csv")
      .cache()
  }

  "TestUpdateCourseMetrics" should "generate reports for 10 batches and validate all scenarios" in {

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF);
      

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .anyNumberOfTimes()
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(externalIdentityDF)

    val storageConfig = StorageConfig("local", "", "src/test/resources/course-metrics")
    EmbeddedES.index("cbatch","_doc","""{"courseId":"do_112726725832507392141","description":"Test Index", "id":"1008"}""", "1008")
    EmbeddedES.index("cbatch","_doc","""{"courseId":"0128448115803914244","participantCount":3,"completedCount":3,"id":"1004"}""", "1004")

//    EmbeddedES.getAllDocuments("cbatch").foreach(f => {
//      Console.println(f)
//    })


    CourseMetricsJob.prepareReport(spark, storageConfig, reporterMock.loadData)

    // TODO: Add assertions here
//    EmbeddedES.getAllDocuments("cbatchstats").foreach(f => {
//      Console.println(f)
//    })
////
//    EmbeddedES.getAllDocuments("cbatch").foreach(f => {
//      Console.println(f)
//    })


    
    (new HadoopFileUtil()).delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/course-metrics")
  }

}