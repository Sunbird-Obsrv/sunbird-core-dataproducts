package org.ekstep.analytics.updater

import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector._
import org.ekstep.analytics.util.WorkFlowUsageSummaryFact
import org.ekstep.analytics.framework.FrameworkContext

class TestUpdateWorkflowUsageDB extends SparkSpec(null) {

    override def beforeAll() {
        super.beforeAll()
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        session.execute("TRUNCATE local_platform_db.workflow_usage_summary_fact");
        session.execute("INSERT INTO local_platform_db.workflow_usage_summary_fact(d_period,d_channel,d_app_id,d_tag,d_type,d_mode,d_device_id,d_content_id,d_user_id,m_publish_date,m_last_sync_date,m_last_gen_date,m_total_ts,m_total_sessions,m_avg_ts_session,m_total_interactions,m_avg_interactions_min,m_total_pageviews_count,m_avg_pageviews,m_total_users_count,m_total_content_count,m_total_devices_count,m_unique_users,m_device_ids,m_contents,m_updated_date) VALUES (20180206, 'in.ekstep', 'no_value', 'all', 'editor', 'edit', '18079404b0d84e4571dda27e8fdefe29', 'do_112399203071664128194', '407', 1517894621901, 1517894621901, 1517894621901, 10, 1, 10, 5, 30.0, 3, 0.3, 0, 0, 0, bigintAsBlob(0), bigintAsBlob(0), bigintAsBlob(0), 1517894621901);");
    }

    override def afterAll() {
        super.afterAll();
    }

    implicit val fc = new FrameworkContext();
    
    it should "update all usage suammary db and check the updated fields" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/workflow-usage-updater/test-data.log");
        val rdd2 = UpdateWorkFlowUsageDB.execute(rdd, None);
        rdd2.count() should be(264)

        // type= 'editor' & period = 0  
        val cumulativeWorkflowSumm = sc.cassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)
            .where("d_period=?", 0)
            .where("d_channel=?", "in.ekstep")
            .where("d_app_id=?", "no_value")
            .where("d_tag=?", "all")
            .where("d_type=?", "editor")
            .where("d_mode=?", "edit")
            .where("d_device_id=?", "18079404b0d84e4571dda27e8fdefe29")
            .where("d_content_id=?", "do_112399203071664128194")
            .where("d_user_id=?", "407").first

        cumulativeWorkflowSumm.m_content_type.get should be("Content")
        cumulativeWorkflowSumm.m_total_ts should be(293.18)
        cumulativeWorkflowSumm.m_total_sessions should be(3)
        cumulativeWorkflowSumm.m_avg_ts_session should be(97.73)
        cumulativeWorkflowSumm.m_total_interactions should be(94)
        cumulativeWorkflowSumm.m_avg_interactions_min should be(19.24)
        cumulativeWorkflowSumm.m_total_pageviews_count should be(3)
        cumulativeWorkflowSumm.m_avg_pageviews should be(1)
        cumulativeWorkflowSumm.m_total_users_count should be(0)
        cumulativeWorkflowSumm.m_total_content_count should be(0)
        cumulativeWorkflowSumm.m_total_devices_count should be(0)

        val cumulativeAcrossAll = sc.cassandraTable[WorkFlowUsageSummaryFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY_FACT)
            .where("d_period=?", 0)
            .where("d_channel=?", "in.ekstep")
            .where("d_app_id=?", "no_value")
            .where("d_tag=?", "all")
            .where("d_type=?", "editor")
            .where("d_mode=?", "edit")
            .where("d_device_id=?", "all")
            .where("d_content_id=?", "all")
            .where("d_user_id=?", "all").first

        cumulativeAcrossAll.m_content_type.get should be("all")
        cumulativeAcrossAll.m_total_ts should be(840.17)
        cumulativeAcrossAll.m_total_sessions should be(12)
        cumulativeAcrossAll.m_avg_ts_session should be(70.01)
        cumulativeAcrossAll.m_total_interactions should be(141)
        cumulativeAcrossAll.m_avg_interactions_min should be(10.07)
        cumulativeAcrossAll.m_total_pageviews_count should be(12)
        cumulativeAcrossAll.m_avg_pageviews should be(1)
        cumulativeAcrossAll.m_total_users_count should be(0)
        cumulativeAcrossAll.m_total_content_count should be(0)
        cumulativeAcrossAll.m_total_devices_count should be(0)
    }
}