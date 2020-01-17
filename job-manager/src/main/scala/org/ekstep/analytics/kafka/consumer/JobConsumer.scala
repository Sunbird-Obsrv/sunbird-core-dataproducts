package org.ekstep.analytics.kafka.consumer

import org.ekstep.analytics.streaming.SimpleKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import java.util.Properties
import java.util.concurrent.BlockingQueue
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._

class JobConsumer(topic: String, consumerProps: Properties, queue: BlockingQueue[String], pollTimeout: Duration = 10 seconds, restartOnExceptionDelay: Duration = SimpleKafkaConsumer.restartOnExceptionDelay) 
        extends SimpleKafkaConsumer(topic, consumerProps, pollTimeout = pollTimeout, restartOnExceptionDelay = restartOnExceptionDelay) {

    override implicit val className = "org.ekstep.analytics.kafka.consumer.JobConsumer"
    
    override protected def processRecords(records: ConsumerRecords[String, String]): Unit = {
        JobLogger.log("Inside processRecords()", None, INFO);
        val messages = records.map(f => f.value());
        JobLogger.log("messages count", Option(Map("count" -> messages.size)), INFO);
        if(messages.size > 0) {
            messages.foreach { x => 
              JobLogger.log("Jobmanager: Queuing job: " + x, None, INFO);
              queue.put(x) 
              };
        }
    }
}

object JobConsumerConfig {

    // Simple helper to create properties from the above. Note that we don't cache the lookup, as it may always change.
    def makeProps(bootStrapServer: String = "localhost:9092", consumerGroup: String = "dev.job-consumer", addProps: Option[Properties] = None,  maxPollRecords: Option[Int] = Option(1), sessionTimeout: String = "6000", heartBeatInterval: String = "10000") = {
        val props = SimpleKafkaConsumer.makeProps(bootStrapServer, consumerGroup, maxPollRecords)
        // Make stuff fail a bit quicker than normal
        props.put("session.timeout.ms", sessionTimeout)
        if(addProps.isDefined && addProps.get.size() > 0)  {
            props.putAll(addProps.get);
        }
        props
    }
}