package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestWorkFlowUsageMetricsUpdater extends SparkSpec(null) {

    "WorkFlowUsageMetricsUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-profile/test-data1.log"))))), None, None, "org.ekstep.analytics.updater.WorkFlowUsageMetricsUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestWorkFlowUsageMetricsUpdater"), Option(false))
        WorkFlowUsageMetricsUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }
}
