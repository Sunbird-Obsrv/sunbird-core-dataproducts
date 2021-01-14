package org.ekstep.analytics.job

import org.ekstep.analytics.framework.V3DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TestJobMonitor extends FlatSpec with Matchers with BeforeAndAfterAll {

    it should "test Job Monitor" in {

        val config = """{"jobsCount":1,"topic":"test","bootStrapServer":"localhost:9092","zookeeperConnect":"localhost:2181","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"dev-data-store","tempFolder":"transient-data-test"}""";
        val jobConfig = JSONUtils.deserialize[JobManagerConfig](config);
        JobMonitor.init(jobConfig);
        val startEvent1 = """{"eid":"JOB_START","ets":1584083811927,"ver":"3.0","mid":"3F06143D1F9D9BE419E45EE0299DFCE9","actor":{"id":"","type":"System"},"context":{"channel":"in.ekstep","pdata":{"id":"AnalyticsDataPipeline","ver":"1.0","pid":"UpdateDeviceProfileDB","model":null},"env":"analytics","sid":null,"did":null,"cdata":null,"rollup":null},"object":null,"edata":{"data":{"config":{"search":{"type":"local","query":null,"queries":[{"bucket":null,"prefix":null,"startDate":null,"endDate":null,"delta":null,"brokerList":null,"topic":null,"windowType":null,"windowDuration":null,"file":"src/test/resources/device-profile/test-data1.log","excludePrefix":null,"datePattern":null,"folder":null,"creationDate":null}],"druidQuery":null},"filters":null,"sort":null,"model":"org.ekstep.analytics.updater.DeviceProfileUpdater","modelParams":null,"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"TestDeviceProfileUpdater","deviceMapping":false,"exhaustConfig":null,"name":null},"model":"UpdateDeviceProfileDB"},"message":"Started processing of UpdateDeviceProfileDB","class":"org.ekstep.analytics.framework.driver.BatchJobDriver","level":"INFO"},"@timestamp":"2020-03-13T07:16:51+00:00"}
                           |""";
        val startMsg1 = JobMonitor.jobStartMsg(JSONUtils.deserialize[V3DerivedEvent](startEvent1))
        startMsg1 should be ("*Job*: `UpdateDeviceProfileDB` | Status: `Started` | Date: ``")

        val startEvent2 = """{"eid":"JOB_START","ets":1584083811927,"ver":"3.0","mid":"3F06143D1F9D9BE419E45EE0299DFCE9","actor":{"id":"","type":"System"},"context":{"channel":"in.ekstep","pdata":{"id":"AnalyticsDataPipeline","ver":"1.0","pid":"UpdateDeviceProfileDB","model":null},"env":"analytics","sid":null,"did":null,"cdata":null,"rollup":null},"object":null,"edata":{"message":"Started processing of UpdateDeviceProfileDB","class":"org.ekstep.analytics.framework.driver.BatchJobDriver","level":"INFO"},"@timestamp":"2020-03-13T07:16:51+00:00"}
                           |""";
        val startMsg2 = JobMonitor.jobStartMsg(JSONUtils.deserialize[V3DerivedEvent](startEvent2))
        startMsg2 should be ("*Job*: `` | Status: `Started` | Date: ``")


        val endEvent = """{"eid":"JOB_END","ets":1584083817419,"ver":"3.0","mid":"9B964E639CA41FDD3E1E9F23EDF5B6F1","actor":{"id":"","type":"System"},"context":{"channel":"in.ekstep","pdata":{"id":"AnalyticsDataPipeline","ver":"1.0","pid":"UpdateDeviceProfileDB","model":null},"env":"analytics","sid":null,"did":null,"cdata":null,"rollup":null},"object":null,"edata":{"data":{"model":"UpdateDeviceProfileDB","inputEvents":3,"timeTaken":5.0,"outputEvents":1},"status":"SUCCESS","message":"UpdateDeviceProfileDB processing complete","class":"org.ekstep.analytics.framework.driver.BatchJobDriver","level":"INFO"},"@timestamp":"2020-03-13T07:16:57+00:00"}
                         |""";
        val endMsg = JobMonitor.jobEndMsg(JSONUtils.deserialize[V3DerivedEvent](endEvent))
        endMsg should be("*Job*: `UpdateDeviceProfileDB` | Status: `SUCCESS` | Date: `` | inputEvents: `3` | outputEvents: `1` | timeTaken: `5.0`")

        val failedEndEvent = """{"eid":"JOB_END","ets":1584083878726,"ver":"3.0","mid":"FD6C9FDE4E5BB7E6BF8D71C5595F6522","actor":{"id":"","type":"System"},"context":{"channel":"in.ekstep","pdata":{"id":"AnalyticsDataPipeline","ver":"1.0","pid":"ExperimentDefinitionModel","model":null},"env":"analytics","sid":null,"did":null,"cdata":null,"rollup":null},"object":null,"edata":{"data":{"model":"ExperimentDefinitionModel","inputEvents":0,"statusMsg":"Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections."},"message":"ExperimentDefinitionModel processing failed","class":"org.ekstep.analytics.framework.driver.BatchJobDriver","level":"INFO"},"@timestamp":"2020-03-13T07:17:58+00:00"}
                               |""";
        val failedEndMsg = JobMonitor.jobEndMsg(JSONUtils.deserialize[V3DerivedEvent](failedEndEvent))
        failedEndMsg should be("*Job*: `ExperimentDefinitionModel` | Status: `FAILED` | Date: `` | inputEvents: `0` | statusMsg: `Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.`")

        val failedEndEvent2 = """{"eid":"JOB_END","ets":1584083878726,"ver":"3.0","mid":"FD6C9FDE4E5BB7E6BF8D71C5595F6522","actor":{"id":"","type":"System"},"context":{"channel":"in.ekstep","pdata":{"id":"AnalyticsDataPipeline","ver":"1.0","pid":"ExperimentDefinitionModel","model":null},"env":"analytics","sid":null,"did":null,"cdata":null,"rollup":null},"object":null,"edata":{"message":"ExperimentDefinitionModel processing failed","class":"org.ekstep.analytics.framework.driver.BatchJobDriver","level":"INFO"},"@timestamp":"2020-03-13T07:17:58+00:00"}
                               |""";
        val failedEndMsg2 = JobMonitor.jobEndMsg(JSONUtils.deserialize[V3DerivedEvent](failedEndEvent2))
        failedEndMsg2 should be("*Job*: `` | Status: `FAILED` | Date: `` | inputEvents: `0` | statusMsg: `NA`")
    }

    it should "test JobEventListener" in {
        val listner = new JobEventListener("#test_channel", "JobManager")
        val startEvent = """{"eid":"JOB_START","ets":1584083811927,"ver":"3.0","mid":"3F06143D1F9D9BE419E45EE0299DFCE9","actor":{"id":"","type":"System"},"context":{"channel":"in.ekstep","pdata":{"id":"AnalyticsDataPipeline","ver":"1.0","pid":"UpdateDeviceProfileDB","model":null},"env":"analytics","sid":null,"did":null,"cdata":null,"rollup":null},"object":null,"edata":{"data":{"config":{"search":{"type":"local","query":null,"queries":[{"bucket":null,"prefix":null,"startDate":null,"endDate":null,"delta":null,"brokerList":null,"topic":null,"windowType":null,"windowDuration":null,"file":"src/test/resources/device-profile/test-data1.log","excludePrefix":null,"datePattern":null,"folder":null,"creationDate":null}],"druidQuery":null},"filters":null,"sort":null,"model":"org.ekstep.analytics.updater.DeviceProfileUpdater","modelParams":null,"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":10,"appName":"TestDeviceProfileUpdater","deviceMapping":false,"exhaustConfig":null,"name":null},"model":"UpdateDeviceProfileDB"},"message":"Started processing of UpdateDeviceProfileDB","class":"org.ekstep.analytics.framework.driver.BatchJobDriver","level":"INFO"},"@timestamp":"2020-03-13T07:16:51+00:00"}
                            |""";
        listner.onMessage(startEvent)

        val endEvent = """{"eid":"JOB_END","ets":1584083817419,"ver":"3.0","mid":"9B964E639CA41FDD3E1E9F23EDF5B6F1","actor":{"id":"","type":"System"},"context":{"channel":"in.ekstep","pdata":{"id":"AnalyticsDataPipeline","ver":"1.0","pid":"UpdateDeviceProfileDB","model":null},"env":"analytics","sid":null,"did":null,"cdata":null,"rollup":null},"object":null,"edata":{"data":{"model":"UpdateDeviceProfileDB","inputEvents":3,"timeTaken":5.0,"outputEvents":1},"status":"SUCCESS","message":"UpdateDeviceProfileDB processing complete","class":"org.ekstep.analytics.framework.driver.BatchJobDriver","level":"INFO"},"@timestamp":"2020-03-13T07:16:57+00:00"}
                         |""";
        listner.onMessage(endEvent)

    }


}
