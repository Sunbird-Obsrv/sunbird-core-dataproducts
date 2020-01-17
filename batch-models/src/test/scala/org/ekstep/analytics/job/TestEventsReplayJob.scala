package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.util.CommonUtil

class TestEventsReplayJob extends SparkSpec(null) {

  val config = "{\"search\":{\"type\":\"local\",\"queries\":[{\"file\":\"src/test/resources/telemetry-replay/data.json\"}]},\"model\":\"org.ekstep.analytics.job.EventsReplayJob\",\"modelParams\":{},\"output\":[{\"to\":\"console\",\"params\":{\"printEvent\":false}}],\"parallelization\":8,\"deviceMapping\":true}"

  "EventsReplayJob" should "Read and dispatch data properly" in {

    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    val input = EventsReplayJob.getInputData(jobConfig)
    input.count() should be(37)

    val output = EventsReplayJob.dispatchData(jobConfig, input)
    output should be(37)

    CommonUtil.closeSparkContext();
    EventsReplayJob.main(config)(None)

  }

}
