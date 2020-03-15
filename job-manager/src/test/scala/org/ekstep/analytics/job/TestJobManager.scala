package org.ekstep.analytics.job

import java.util.concurrent.CountDownLatch

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.kafka.consumer.{JobConsumerV2, JobConsumerV2Config}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TestJobManager extends FlatSpec with Matchers with BeforeAndAfterAll with EmbeddedKafka {

    it should "start job manager and consume 1 message" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            createCustomTopic("output", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()

            publishStringMessageToKafka("test", """{"model":"wfs","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")
            println(consumeFirstStringMessageFrom("test"))

            val config = """{"jobsCount":1,"topic":"test","bootStrapServer":"localhost:9092","zookeeperConnect":"localhost:2181","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"dev-data-store","tempFolder":"transient-data-test"}""";
            JobManager.main(config);


        }
    }

    it should "test JobRunner" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()

            publishStringMessageToKafka("test", """{"model":"abc","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")

            val strConfig = """{"jobsCount":1,"topic":"test","bootStrapServer":"localhost:9092","zookeeperConnect":"localhost:2181","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"dev-data-store","tempFolder":"transient-data-test"}""";
            val config = JSONUtils.deserialize[JobManagerConfig](strConfig)

            val doneSignal = new CountDownLatch(1);
            val props = JobConsumerV2Config.makeProps("localhost:2181", "test-jobmanager")
            val consumer = new JobConsumerV2("test", props);
            val runner = new JobRunner(config, consumer, doneSignal)
            runner.run()
//            Thread.sleep(10 * 1000);

//            publishStringMessageToKafka("test", """{"model":"abc","config":{"search":{"type":"none"},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")
            consumer.close()
        }
    }

}
