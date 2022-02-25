package org.ekstep.analytics.kafka.consumer

import java.util.Properties
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConverters._


class JobConsumerV2(topic: String, consumerProps: Properties) {

    implicit val className = "org.ekstep.analytics.kafka.consumer.JobConsumerV2"
//    private val config = new ConsumerConfig(consumerProps)
    private val connector = new KafkaConsumer[String, String](consumerProps)
    connector.subscribe(java.util.Collections.singletonList(topic))
//    private val filterSpec = new Whitelist(topic)
    private var streams = connector.poll(60).asScala

    var iterator = streams.toIterator

    def read(): Option[String] =
        try {
            streams = connector.poll(60).asScala
            iterator = streams.toIterator
            JobLogger.log("Consumer details: " + consumerProps + " Iterator: " + iterator + " streams: " + streams, None, INFO);
            if (hasNext) {
                JobLogger.log("Getting message from queue.", None, INFO);
                val message = iterator.next().value()
                Some(new String(message))
            } else {
                JobLogger.log("Waiting for message from queue", None, INFO);
                None
            }
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
                JobLogger.log("Exception reading message from queue: " + ex.getMessage, None, INFO);
                None
        }

    private def hasNext(): Boolean =
        try {
            val check = iterator.hasNext
            JobLogger.log("hasNext have some value or not: " + check, None, INFO);
            check
        } catch {
//            case timeOutEx: ConsumerTimeoutException =>
//                false
            case ex: Exception =>
                JobLogger.log("Exception reading message from queue: " + ex.getMessage, None, INFO);
                JobLogger.log("Getting error when reading message", Option(Map("err" -> ex.getMessage)), ERROR);
                false
        }

    def close(): Unit = connector.close()

}

object JobConsumerV2Config {

    // Simple helper to create properties from the above. Note that we don't cache the lookup, as it may always change.
    def makeProps(brokerConnect: String = "localhost:9092", zookeeperConnect: String = "localhost:2181", consumerGroup: String = "dev.job-consumer", consumerTimeoutMs: String = "120000") = {
        val props = new Properties()
        props.put("group.id", consumerGroup)
        props.put("bootstrap.servers", brokerConnect)
//        props.put("zookeeper.connect", zookeeperConnect)
        props.put("key.deserializer", classOf[StringDeserializer])
        props.put("value.deserializer", classOf[StringDeserializer])
        props.put("auto.offset.reset", "earliest")
        //2 minute consumer timeout
        props.put("consumer.timeout.ms", consumerTimeoutMs)
        //commit after each 10 second
        props.put("auto.commit.interval.ms", "10000")
        props
    }
}