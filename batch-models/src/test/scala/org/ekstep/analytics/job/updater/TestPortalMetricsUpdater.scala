package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.{ JobConfig, Fetcher, Query, Dispatcher }
import org.ekstep.analytics.framework.util.JSONUtils

class TestPortalMetricsUpdater extends SparkSpec(null) {

  "PortalMetricsUpdater" should "execute the job and shouldn't throw any exception" in {
    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/device-profile/test-data1.log"))))), None, None, "org.ekstep.analytics.updater.PortalMetricsUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestPortalMetricsUpdater"), Option(false))
    PortalMetricsUpdater.main(JSONUtils.serialize(config))(Option(sc));
  }
}
