package org.ekstep.analytics.model
/**
 * @author Yuva
 */
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.framework.FrameworkContext

class TestMonitorSummaryModel extends SparkSpec(null) {

    "Monitor Summary Model" should "monitor the data products logs" in {
      
        implicit val fc = new FrameworkContext();
        val modelMapping = loadFile[ModelMapping]("src/test/resources/monitor-summary/model-mapping.log").collect().toList;
        val rdd1 = loadFile[V3Event]("src/test/resources/monitor-summary/2017-12-08.log");
        val rdd2 = MonitorSummaryModel.execute(rdd1, Option(Map("model" -> modelMapping)));
        val eks_map = rdd2.first().edata.eks.asInstanceOf[Map[String, AnyRef]]
        eks_map.get("jobs_completed_count").get.asInstanceOf[Number].longValue() should be(72)
        eks_map.get("total_events_generated").get.asInstanceOf[Number].longValue() should be(36742)
        eks_map.get("jobs_failed_count").get.asInstanceOf[Number].longValue() should be(1)
        eks_map.get("total_ts").get.asInstanceOf[Number].doubleValue() should be(222965.0)
        eks_map.get("jobs_start_count").get.asInstanceOf[Number].longValue() should be(73)
    }
}