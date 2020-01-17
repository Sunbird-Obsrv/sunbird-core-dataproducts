package org.ekstep.analytics.job

import org.ekstep.analytics.framework.util.JSONUtils
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ArrayBlockingQueue

import org.ekstep.analytics.kafka.consumer.JobConsumer
import org.ekstep.analytics.kafka.consumer.JobConsumerConfig
import java.util.concurrent.Executors

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import java.util.concurrent.CountDownLatch

import org.ekstep.analytics.framework.util.EventBusUtil
import com.google.common.eventbus.Subscribe
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.JobConfig
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{ StorageConfig, StorageServiceFactory }
import org.ekstep.analytics.kafka.consumer.JobConsumerV2Config
import org.ekstep.analytics.kafka.consumer.JobConsumerV2
import org.ekstep.analytics.framework.FrameworkContext

case class JobManagerConfig(jobsCount: Int, topic: String, bootStrapServer: String, zookeeperConnect: String, consumerGroup: String, slackChannel: String, slackUserName: String, tempBucket: String, tempFolder: String, runMode: String = "shutdown");

object JobManager extends optional.Application {

    implicit val className = "org.ekstep.analytics.job.JobManager";
    val storageType = AppConf.getStorageType()
    val storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, AppConf.getStorageKey(storageType), AppConf.getStorageSecret(storageType)))

    var jobsCompletedCount = 0;

    def main(config: String) {
        JobLogger.init("JobManager");
        val jobManagerConfig = JSONUtils.deserialize[JobManagerConfig](config);
        JobLogger.log("Starting job manager", Option(Map("config" -> jobManagerConfig)), INFO);
        init(jobManagerConfig);
    }

    def init(config: JobManagerConfig) = {
        storageService.deleteObject(config.tempBucket, config.tempFolder, Option(true));
        val jobQueue: BlockingQueue[String] = new ArrayBlockingQueue[String](config.jobsCount);
        val consumer = initializeConsumer(config, jobQueue);
        JobLogger.log("Initialized the job consumer", None, INFO);
        val executor = Executors.newFixedThreadPool(1);
        JobLogger.log("Total job count: " + config.jobsCount, None, INFO);
        val doneSignal = new CountDownLatch(config.jobsCount);
        JobMonitor.init(config);
        JobLogger.log("Initialized the job event listener. Starting the job executor", None, INFO);
        executor.submit(new JobRunner(config, consumer, doneSignal));

        doneSignal.await();
        JobLogger.log("Job manager execution completed. Shutting down the executor and consumer", None, INFO);
        executor.shutdown();
        JobLogger.log("Job manager executor shutdown completed", None, INFO);
        consumer.close();
        JobLogger.log("Job manager consumer shutdown completed", None, INFO);
    }

    private def initializeConsumer(config: JobManagerConfig, jobQueue: BlockingQueue[String]): JobConsumerV2 = {
        val props = JobConsumerV2Config.makeProps(config.zookeeperConnect, config.consumerGroup)
        val consumer = new JobConsumerV2(config.topic, props);
        consumer;
    }
}

class JobRunner(config: JobManagerConfig, consumer: JobConsumerV2, doneSignal: CountDownLatch) extends Runnable {

    implicit val className: String = "JobRunner";

    override def run {
        implicit val fc = new FrameworkContext();
        // Register the storage service for all data
        fc.getStorageService(AppConf.getConfig("cloud_storage_type"));
        // Register the reports storage service
        fc.getStorageService(AppConf.getConfig("cloud_storage_type"), AppConf.getConfig("reports_azure_storage_key"), AppConf.getConfig("reports_azure_storage_secret"));
        while (doneSignal.getCount() != 0) {
            val record = consumer.read;
            if (record.isDefined) {
                JobLogger.log("Starting execution of " + record, None, INFO);
                executeJob(record.get);
                doneSignal.countDown();
            } else {
                Thread.sleep(10 * 1000); // Sleep for 10 seconds
            }
        }
        // Jobs are done. Close the framework context.
        fc.closeContext();
    }

    private def executeJob(record: String)(implicit fc: FrameworkContext) {
        JobLogger.log("Starting the job execution", None, INFO);
        val jobConfig = JSONUtils.deserialize[Map[String, AnyRef]](record);
        val modelName = jobConfig.get("model").get.toString()
        val configStr = JSONUtils.serialize(jobConfig.get("config").get)
        val config = JSONUtils.deserialize[JobConfig](configStr)
        try {
            if (StringUtils.equals("data-exhaust", modelName)) {
                val modelParams = config.modelParams.get
                val delayFlag = modelParams.get("shouldDelay").get.asInstanceOf[Boolean]
                val delayTime = modelParams.get("delayInMilis").get.asInstanceOf[Number].longValue()
                if (delayFlag) {
                    Thread.sleep(delayTime)
                }
            }
            JobLogger.log("Executing " + modelName, None, INFO);
            JobExecutorV2.main(modelName, configStr)
            JobLogger.log("Finished executing " + modelName, None, INFO);
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
        }
    }
}

object TestJobManager {
    def main(args: Array[String]): Unit = {
        val config = """{"jobsCount":2,"topic":"local.analytics.job_queue","bootStrapServer":"localhost:9092","consumerGroup":"jobmanager","slackChannel":"#test_channel","slackUserName":"JobManager","tempBucket":"ekstep-dev-data-store","tempFolder":"transient-data"}""";
        JobManager.main(config);
    }
}