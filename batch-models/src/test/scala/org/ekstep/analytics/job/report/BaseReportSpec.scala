package org.ekstep.analytics.job.report

import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.util.EmbeddedES
import org.ekstep.analytics.util.EsIndex
import org.joda.time.DateTimeUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

class BaseReportSpec extends BaseSpec with BeforeAndAfterAll {

  override def beforeAll() {
    DateTimeUtils.setCurrentMillisFixed(1531047600000L);
    super.beforeAll();
    Console.println("****** Starting embedded elasticsearch service ******");
    val spark = getSparkSession();
    val csMapping = spark.sparkContext.textFile("src/test/resources/reports/compositesearch_mapping.json", 1).collect().head;
    val cbatchAliasMapping = spark.sparkContext.textFile("src/test/resources/reports/cbatch_alias_mapping.json", 1).collect().head;
    val cbatchAssessmentAliasMapping = spark.sparkContext.textFile("src/test/resources/reports/cbatch_assessment_alias_mapping.json", 1).collect().head;
    
    EmbeddedES.start(
      Array(
        EsIndex("compositesearch", Option("cs"), Option(csMapping), None),
        EsIndex("cbatch", None, None, Option(cbatchAliasMapping)),
        EsIndex("cbatch-assessment-08-07-2018-11-00", None, None, Option(cbatchAssessmentAliasMapping))))
  }

  override def afterAll() {
    //super.afterAll();
    EmbeddedES.stop()
  }
  
}