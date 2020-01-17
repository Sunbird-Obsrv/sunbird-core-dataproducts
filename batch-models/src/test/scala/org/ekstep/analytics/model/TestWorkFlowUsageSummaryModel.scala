package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.FrameworkContext

class TestWorkFlowUsageSummaryModel extends SparkSpec(null) {

    implicit val fc = new FrameworkContext();  
  
    "WorkFlowUsageSummaryModel" should "generate 2 workflow usage summary events per user and all user" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/workflow-usage-summary/test-data1.log");
        val rdd2 = WorkFlowUsageSummaryModel.execute(rdd1, None);

        rdd2.count() should be(2)
        val me = rdd2.collect();

        // check for all usage session summary
        val event1 = me.filter(f => f.mid.equals("D899338AA10C72EC0CF4344A4A1E05A9")).last

        event1.eid should be("ME_WORKFLOW_USAGE_SUMMARY");
        event1.context.pdata.model.get should be("WorkFlowUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("session");
        event1.dimensions.content_id.get should be("all");
        event1.dimensions.content_type.get should be("all");
        event1.dimensions.tag.get should be("all");
        event1.dimensions.did.get should be("all");
        event1.dimensions.uid.get should be("all");
        event1.dimensions.period.get should be(20180123);
        event1.dimensions.channel.get should be("in.ekstep");
        event1.dimensions.mode.get should be("")

        val summary1 = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(event1.edata.eks));

        summary1.get("total_users_count").get should be(1);
        summary1.get("total_devices_count").get should be(0);
        summary1.get("total_content_count").get should be(0);
        summary1.get("avg_ts_session").get should be(1249.0);
        summary1.get("total_sessions").get should be(2);
        summary1.get("avg_interactions_min").get should be(1.01);
        summary1.get("total_interactions").get should be(42);
        summary1.get("avg_pageviews").get should be(1.0);
        summary1.get("total_ts").get should be(2498.0);
        summary1.get("total_pageviews_count").get should be(2);

        // check for per user summary
        val event2 = me.filter(f => f.mid.equals("8FEA0EF5EC6709ACB24C4AE59DA307BE")).last

        event2.eid should be("ME_WORKFLOW_USAGE_SUMMARY");
        event2.context.pdata.model.get should be("WorkFlowUsageSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("DAY");
        event2.context.date_range should not be null;
        event2.dimensions.`type`.get should be("session");
        event2.dimensions.content_id.get should be("all");
        event2.dimensions.content_type.get should be("all");
        event2.dimensions.tag.get should be("all");
        event2.dimensions.did.get should be("all");
        event2.dimensions.uid.get should be("427");
        event2.dimensions.period.get should be(20180123);
        event2.dimensions.channel.get should be("in.ekstep");
        event2.dimensions.mode.get should be("")

        val summary2 = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(event2.edata.eks));

        summary2.get("total_users_count").get should be(0);
        summary2.get("total_devices_count").get should be(0);
        summary2.get("total_content_count").get should be(0);
        summary2.get("avg_ts_session").get should be(1249.0);
        summary2.get("total_sessions").get should be(2);
        summary2.get("avg_interactions_min").get should be(1.01);
        summary2.get("total_interactions").get should be(42);
        summary2.get("avg_pageviews").get should be(1.0);
        summary2.get("total_ts").get should be(2498.0);
        summary2.get("total_pageviews_count").get should be(2);

    }
    
    it should "generate 8 workflow usage summary events" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/workflow-usage-summary/test-data2.log");
        val rdd2 = WorkFlowUsageSummaryModel.execute(rdd1, None);

        rdd2.count() should be(8)
        val me = rdd2.collect();

        // check for all usage player summary
        val event1 = me.filter(f => f.mid.equals("C53E7E06EF053C4576E116D13EEF0FE6")).last

        event1.eid should be("ME_WORKFLOW_USAGE_SUMMARY");
        event1.context.pdata.model.get should be("WorkFlowUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("player");
        event1.dimensions.content_id.get should be("all");
        event1.dimensions.content_type.get should be("all");
        event1.dimensions.tag.get should be("all");
        event1.dimensions.did.get should be("all");
        event1.dimensions.uid.get should be("all");
        event1.dimensions.period.get should be(20180108);
        event1.dimensions.channel.get should be("in.ekstep");
        event1.dimensions.mode.get should be("play")

        val summary1 = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(event1.edata.eks));

        summary1.get("total_users_count").get should be(1);
        summary1.get("total_devices_count").get should be(1);
        summary1.get("total_content_count").get should be(1);
        summary1.get("avg_ts_session").get should be(123.46);
        summary1.get("total_sessions").get should be(2);
        summary1.get("avg_interactions_min").get should be(5.83);
        summary1.get("total_interactions").get should be(24);
        summary1.get("avg_pageviews").get should be(2.0);
        summary1.get("total_ts").get should be(246.91);
        summary1.get("total_pageviews_count").get should be(5);
        
        // check for per device usage player summary
        val event2 = me.filter(f => f.mid.equals("B7CDCEB0D1F7E8EF55A49F41B62914DF")).last

        event2.eid should be("ME_WORKFLOW_USAGE_SUMMARY");
        event2.context.pdata.model.get should be("WorkFlowUsageSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("DAY");
        event2.context.date_range should not be null;
        event2.dimensions.`type`.get should be("player");
        event2.dimensions.content_id.get should be("all");
        event2.dimensions.content_type.get should be("all");
        event2.dimensions.tag.get should be("all");
        event2.dimensions.did.get should be("11573c50cae2078e847f12c91a2d1965eaa73714");
        event2.dimensions.uid.get should be("all");
        event2.dimensions.period.get should be(20180108);
        event2.dimensions.channel.get should be("in.ekstep");
        event2.dimensions.mode.get should be("play")

        val summary2 = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(event2.edata.eks));

        summary2.get("total_users_count").get should be(1);
        summary2.get("total_devices_count").get should be(0);
        summary2.get("total_content_count").get should be(1);
        summary2.get("avg_ts_session").get should be(123.46);
        summary2.get("total_sessions").get should be(2);
        summary2.get("avg_interactions_min").get should be(5.83);
        summary2.get("total_interactions").get should be(24);
        summary2.get("avg_pageviews").get should be(2.0);
        summary2.get("total_ts").get should be(246.91);
        summary2.get("total_pageviews_count").get should be(5);
        
        // check for per device per content usage player summary
        val event3 = me.filter(f => f.mid.equals("FE9567B7603146E19CF4FE8EF66D2D66")).last

        event3.eid should be("ME_WORKFLOW_USAGE_SUMMARY");
        event3.context.pdata.model.get should be("WorkFlowUsageSummarizer");
        event3.context.pdata.ver should be("1.0");
        event3.context.granularity should be("DAY");
        event3.context.date_range should not be null;
        event3.dimensions.`type`.get should be("player");
        event3.dimensions.content_id.get should be("do_30094761");
        event3.dimensions.content_type.get should be("Content");
        event3.dimensions.tag.get should be("all");
        event3.dimensions.did.get should be("11573c50cae2078e847f12c91a2d1965eaa73714");
        event3.dimensions.uid.get should be("all");
        event3.dimensions.period.get should be(20180108);
        event3.dimensions.channel.get should be("in.ekstep");
        event3.dimensions.mode.get should be("play")

        val summary3 = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(event3.edata.eks));

        summary3.get("total_users_count").get should be(1);
        summary3.get("total_devices_count").get should be(0);
        summary3.get("total_content_count").get should be(0);
        summary3.get("avg_ts_session").get should be(123.46);
        summary3.get("total_sessions").get should be(2);
        summary3.get("avg_interactions_min").get should be(5.83);
        summary3.get("total_interactions").get should be(24);
        summary3.get("avg_pageviews").get should be(2.0);
        summary3.get("total_ts").get should be(246.91);
        summary3.get("total_pageviews_count").get should be(5);
        
    }
    
    it should "generate 16 workflow usage summary events including tag summaries" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.registered_tags");
        }

        val tag1 = RegisteredTag("piwik_json", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/workflow-usage-summary/test-data3.log");
        val rdd2 = WorkFlowUsageSummaryModel.execute(rdd1, None);
        
        rdd2.count() should be(16)
        val me = rdd2.collect();

        // check for per tag usage player summary
        val event1 = me.filter(f => f.mid.equals("EA9289094096EE18DB3F93299D772285")).last

        event1.eid should be("ME_WORKFLOW_USAGE_SUMMARY");
        event1.context.pdata.model.get should be("WorkFlowUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.`type`.get should be("player");
        event1.dimensions.content_id.get should be("all");
        event1.dimensions.content_type.get should be("all");
        event1.dimensions.tag.get should be("piwik_json");
        event1.dimensions.did.get should be("all");
        event1.dimensions.uid.get should be("all");
        event1.dimensions.period.get should be(20180108);
        event1.dimensions.channel.get should be("in.tnpilot");
        event1.dimensions.mode.get should be("play")

        val summary1 = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(event1.edata.eks));

        summary1.get("total_users_count").get should be(1);
        summary1.get("total_devices_count").get should be(1);
        summary1.get("total_content_count").get should be(1);
        summary1.get("avg_ts_session").get should be(225.63);
        summary1.get("total_sessions").get should be(1);
        summary1.get("avg_interactions_min").get should be(3.19);
        summary1.get("total_interactions").get should be(12);
        summary1.get("avg_pageviews").get should be(0);
        summary1.get("total_ts").get should be(225.63);
        summary1.get("total_pageviews_count").get should be(0);
        
    }
}