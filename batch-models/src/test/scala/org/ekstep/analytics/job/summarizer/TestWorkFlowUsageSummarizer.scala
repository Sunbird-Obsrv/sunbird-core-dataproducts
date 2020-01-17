package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestWorkFlowUsageSummarizer extends SparkSpec(null) {
  
    "WorkFlowUsageSummarizer" should "execute WorkFlowUsageSummarizer job and won't throw any Exception" in {

        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/workflow-usage-summary/test-data1.log"))))), null, null, "org.ekstep.analytics.model.WorkFlowUsageSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestWorkFlowUsageSummarizer"), Option(true))
        WorkFlowUsageSummarizer.main(JSONUtils.serialize(config))(Option(sc));
    }
}