package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.FrameworkContext

class TestUpdateWorkFlowUsageMetricsModel extends SparkSpec(null) {

    "UpdateWorkFlowUsageMetricsModel" should "Should execute without exception " in {
        implicit val fc = new FrameworkContext();
        val rdd = loadFile[DerivedEvent]("src/test/resources/device-profile/test-data3.log");
        UpdateWorkFlowUsageMetricsModel.execute(rdd, None)
    }
}
