package org.ekstep.analytics.job

import org.ekstep.analytics.framework.util.JSONUtils
import java.util.concurrent._

import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework.JobConfig
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.ekstep.analytics.kafka.consumer.JobConsumerV2Config
import org.ekstep.analytics.kafka.consumer.JobConsumerV2
import org.ekstep.analytics.framework.FrameworkContext
import java.util.concurrent.atomic.AtomicBoolean
import org.ekstep.analytics.framework.conf.AppConf

case class JobManagerConfig(jobsCount: Int, topic: String, bootStrapServer: String, zookeeperConnect: String, consumerGroup: String, slackChannel: String, slackUserName: String, tempBucket: String, tempFolder: String, runMode: String = "shutdown");

object JobManager extends optional.Application {

    implicit val className = "org.ekstep.analytics.job.JobManager";
    val storageType = AppConf.getConfig("cloud_storage_type")
    val storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config")))

    def main(config: String) {
        JobLogger.init("JobManager");
        val jobManagerConfig = JSONUtils.deserialize[JobManagerConfig](config);
        JobLogger.log("Starting job manager", Option(Map("config" -> jobManagerConfig)), INFO);
        init(jobManagerConfig);
    }

    def init(config: JobManagerConfig) = {
        try {
            storageService.deleteObject(config.tempBucket, config.tempFolder, Option(true));
        }
        catch {
            case ex: Exception =>
                ex.printStackTrace()
        }
        val jobQueue: BlockingQueue[String] = new ArrayBlockingQueue[String](config.jobsCount);
        val consumer = initializeConsumer(config, jobQueue);
        JobLogger.log("Initialized the job consumer", None, INFO);
        val executor = Executors.newFixedThreadPool(1);
        JobLogger.log("Total job count: " + config.jobsCount, None, INFO);
        val doneSignal = new CountDownLatch(1);
        JobMonitor.init(config);
        JobLogger.log("Initialized the job event listener. Starting the job executor", None, INFO);
        executor.submit(new JobRunner(config, consumer, doneSignal));
        doneSignal.await()

        JobLogger.log("Job manager execution completed. Shutting down the executor and consumer", None, INFO);
        executor.shutdown();
        JobLogger.log("Job manager executor shutdown completed", None, INFO);
        consumer.close();
        JobLogger.log("Job manager consumer shutdown completed", None, INFO);
    }

    private def initializeConsumer(config: JobManagerConfig, jobQueue: BlockingQueue[String]): JobConsumerV2 = {
        JobLogger.log("Initializing the job consumer", None, INFO);
        val props = JobConsumerV2Config.makeProps(config.zookeeperConnect, config.consumerGroup)
        val consumer = new JobConsumerV2(config.topic, props);
        consumer;
    }
}

class JobRunner(config: JobManagerConfig, consumer: JobConsumerV2, doneSignal: CountDownLatch) extends Runnable {

    implicit val className: String = "JobRunner";

    private val running = new AtomicBoolean(false)
    running.set(true)

    def stop(): Unit = {
        running.set(false)
        doneSignal.countDown();
    }

    override def run {
        implicit val fc = new FrameworkContext();
        // Register the storage service for all data
        fc.getStorageService(AppConf.getConfig("cloud_storage_type"), AppConf.getConfig("storage.key.config"), AppConf.getConfig("storage.secret.config"));
        // Register the reports storage service
        fc.getStorageService(AppConf.getConfig("cloud_storage_type"), AppConf.getConfig("reports.storage.key.config"), AppConf.getConfig("reports.storage.secret.config"));

        while(running.get()) {
            val record = consumer.read;
            if (record.isDefined) {
                JobLogger.log("Starting execution of " + record, None, INFO);
                executeJob(record.get);
                if (record.get.contains("monitor-job-summ"))
                    stop();
            } else {
                // $COVERAGE-OFF$ Code is unreachable
                Thread.sleep(10 * 1000); // Sleep for 10 seconds
                // $COVERAGE-ON$
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
            JobLogger.log("Executing " + modelName, None, INFO);
            JobExecutorV2.main(modelName, configStr)
            JobLogger.log("Finished executing " + modelName, None, INFO);
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
        }
    }
}