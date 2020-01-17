package org.ekstep.analytics.kafka.consumer

import java.util.Properties

import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.JobLogger
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerTimeoutException, Whitelist}
import kafka.serializer.StringDecoder

class JobConsumerV2(topic: String, consumerProps: Properties) {

    implicit val className = "org.ekstep.analytics.kafka.consumer.JobConsumerV2"
    private val config = new ConsumerConfig(consumerProps)
    private val connector = Consumer.create(config)
    private val filterSpec = new Whitelist(topic)
    private val streams = connector.createMessageStreamsByFilter(filterSpec, 1, new StringDecoder(), new StringDecoder())(0)

    lazy val iterator = streams.iterator()

    def read(): Option[String] =
        try {
            if (hasNext) {
                JobLogger.log("Getting message from queue", None, INFO);
                val message = iterator.next().message()
                Some(new String(message))
            } else {
                None
            }
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
                None
        }

    private def hasNext(): Boolean =
        try
            iterator.hasNext()
        catch {
            case timeOutEx: ConsumerTimeoutException =>
                false
            case ex: Exception =>
                JobLogger.log("Getting error when reading message", Option(Map("err" -> ex.getMessage)), ERROR);
                false
        }

    def close(): Unit = connector.shutdown()

}

object JobConsumerV2Config {

    // Simple helper to create properties from the above. Note that we don't cache the lookup, as it may always change.
    def makeProps(zookeeperConnect: String = "localhost:2181", consumerGroup: String = "dev.job-consumer") = {
        val props = new Properties()
        props.put("group.id", consumerGroup)
        props.put("zookeeper.connect", zookeeperConnect)
        props.put("auto.offset.reset", "smallest")
        //2 minute consumer timeout
        props.put("consumer.timeout.ms", "120000")
        //commit after each 10 second
        props.put("auto.commit.interval.ms", "10000")
        props
    }
}