package org.ekstep.analytics.kafka.consumer

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TestJobConsumer extends FlatSpec with Matchers with BeforeAndAfterAll with EmbeddedKafka {

    it should "start job consumer and consume 1 message" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()
            publishStringMessageToKafka("test", """{"model":"wfs","config":{"search":{"type":"local","queries":[{"file":"src/test/resources/workflow-summary/test-data1.log"}]},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")

            val props = JobConsumerV2Config.makeProps("localhost:2181", "test-jobmanager")
            val consumer = new JobConsumerV2("test", props);
            val record = consumer.read;
            record.isDefined should be(true)
            record.get should be("""{"model":"wfs","config":{"search":{"type":"local","queries":[{"file":"src/test/resources/workflow-summary/test-data1.log"}]},"model":"org.ekstep.analytics.model.WorkflowSummary","modelParams":{"apiVersion":"v2"},"output":[{"to":"console","params":{"printEvent": true}},{"to":"kafka","params":{"brokerList":"localhost:9092","topic":"output"}}],"parallelization":8,"appName":"Workflow Summarizer","deviceMapping":true}}""")
            consumer.close()
        }
    }

    it should "start job consumer and test for timeout exception" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()

            val props = JobConsumerV2Config.makeProps("localhost:2181", "test-jobmanager", "6000")
            val consumer = new JobConsumerV2("test", props);
            val record = consumer.read;
            record.isDefined should be(false)
            consumer.close()
        }
    }

    it should "start job consumer and consume null message" in {

        val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
        withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

            Console.println("Kafka started...");
            createCustomTopic("test", Map(), 1, 1)
            implicit val serializer = new StringSerializer()
            implicit val deserializer = new StringDeserializer()
            publishStringMessageToKafka("test", null)

            val props = JobConsumerV2Config.makeProps("localhost:2181", "test-jobmanager")
            val consumer = new JobConsumerV2("test", props);
            val record = consumer.read;
            record.isDefined should be(false)
            consumer.close()
        }
    }

    it should "start job consumer and test for exception case" in {

            val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
            withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>

                Console.println("Kafka started...");
                createCustomTopic("test", Map(), 1, 1)
                implicit val serializer = new StringSerializer()
                implicit val deserializer = new StringDeserializer()
                publishStringMessageToKafka("test", "test-message")

                val props = JobConsumerV2Config.makeProps("localhost:2181", "test-jobmanager")
                val consumer = new JobConsumerV2("test", props);
                val record = consumer.read;
                record.isDefined should be(true)
                consumer.close()
                val record2 = consumer.read;
                record2.isDefined should be(false)
            }

    }

}
