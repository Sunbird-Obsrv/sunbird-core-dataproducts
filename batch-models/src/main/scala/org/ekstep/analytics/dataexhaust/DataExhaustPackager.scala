/**
 * @author Jitendra Singh Sankhwar, Mahesh, Amit
 */
package org.ekstep.analytics.dataexhaust

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.Date
import java.util.UUID

import scala.reflect.runtime.universe
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
//import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.JobRequest
import org.ekstep.analytics.util.RequestConfig
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.toSparkContextFunctions
import com.google.gson.GsonBuilder
import com.datastax.spark.connector.SomeColumns
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.ekstep.analytics.framework.FrameworkContext

/**
 * Case class to hold the manifest json
 */
case class FileInfo(path: String, event_count: Long)
case class ManifestFile(id: String, ver: String, ets: Long, request_id: String, dataset_id: String, total_event_count: Long, first_event_date: String, last_event_date: String, file_info: Array[FileInfo], request: String)
case class Response(request_id: String, client_key: String, job_id: String, metadata: ManifestFile, location: String, stats: Map[String, Any], jobRequest: JobRequest)
case class DataExhaustPackage(request_id: String, client_key: String, job_id: String, execution_time: Long, status: String = "FAILED", latency: Long = 0, dt_job_completed: Option[DateTime] = None, location: Option[String] = None, file_size: Option[Long] = None, dt_file_created: Option[DateTime] = None, dt_expiration: Option[DateTime] = None)
case class EventData(eventName: String, data: RDD[String]);
/**
 * @Packager
 *
 * DataExhaustPackager
 *
 * Functionality
 * 1. Package the all the events based upon their identifer either in json or csv format
 * Events used - consumption-raw, consumption-summary, eks-consumption-metrics, creation-raw, eks-creation-summary and eks-creation-metrics
 */
object DataExhaustPackager extends optional.Application {

    implicit val className = "org.ekstep.analytics.model.DataExhaustPackager"
    implicit val fc = new FrameworkContext();
    
    def name: String = "DataExhaustPackager"
    val storageType = AppConf.getStorageType()
    val storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, AppConf.getStorageKey(storageType), AppConf.getStorageSecret(storageType)))

    def execute()(implicit sc: SparkContext) = {

        val jobRequests = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).where("status = ?", "PENDING_PACKAGING").collect;

		jobRequests.map { request =>
			packageExhaustData(request)
		}
	}

	/**
     * To filter the invalid files
     * @param filePath
     * @return true if file is valid and false for invalid file
     */
    private def invalidFileFilter(filePath: String): Boolean = {
        filePath.endsWith("_SUCCESS") || filePath.endsWith("$folder$") || filePath.endsWith(".crc");
    }

    private def downloadSourceData(saveType: String, requestId: String) {
        val source = requestDataSourcePath(requestId);
        val destination = requestDataLocalPath(requestId)
        saveType match {
            case "s3" | "azure" =>
                val bucket = AppConf.getConfig("data_exhaust.save_config.bucket")
                storageService.download(bucket, source, destination, Option(true))
            case "local" =>
                FileUtils.copyDirectory(new File(source), new File(destination));
        }
    }

    private def getEventsData(requestDataLocalPath: String)(implicit sc: SparkContext): Array[EventData] = {
        val fileObj = new File(requestDataLocalPath)
        fileObj.listFiles.map { dir =>
            EventData(dir.getName, getEventData(dir));
        }
    }

    /**
     * Read all file from a directory and merge into one RDD Type
     * @param dir file directory
     * @return RDD[String] of merge files
     */
    def getEventData(dir: File)(implicit sc: SparkContext): RDD[String] = {
        val data = dir.listFiles().map { x =>
            sc.textFile(x.getAbsolutePath)
        }
        sc.union(data.toSeq)
    }

    private def uploadZipFile(saveType: String, requestId: String, clientKey: String)(implicit sc: SparkContext): (String, Map[String, Any]) = {
        val zipfilePath = zipFileAbsolutePath(requestId)
        val prefix = AppConf.getConfig("data_exhaust.save_config.prefix")
        val deleteSource = AppConf.getConfig("data_exhaust.delete_source");
        val fileStats = saveType match {
            case "s3" | "azure" =>
                val bucket = AppConf.getConfig("data_exhaust.save_config.bucket")
                //val publicS3URL = AppConf.getConfig("data_exhaust.save_config.public_s3_url")
                val compressExtn = ".zip"
                val signedURL = DataExhaustUtils.uploadZip(bucket, prefix, compressExtn, zipfilePath, requestId, clientKey)
                val filePrefix = prefix + requestId + compressExtn
                val stats = storageService.getObject(bucket, filePrefix).metadata;
                if ("true".equals(deleteSource)) DataExhaustUtils.deleteFile(bucket, prefix, Array(requestId))
                //(publicS3URL + "/" + bucket + "/" + s3FilePrefix, stats)
                (signedURL, stats)
            case "local" =>
                val file = new File(zipfilePath)
                val dateTime = new Date(file.lastModified())
                val stats = Map("createdDate" -> dateTime, "size" -> file.length())
                if ("true".equals(deleteSource)) CommonUtil.deleteDirectory(prefix + requestId);
                (zipfilePath, stats)
        }
        fileStats;
    }

    private def requestDataLocalPath(requestId: String): String = {
        val tmpFolderPath = AppConf.getConfig("data_exhaust.save_config.local_path")
        return tmpFolderPath + requestId + "/"
    };
    private def requestDataSourcePath(requestId: String): String = {
        val prefix = AppConf.getConfig("data_exhaust.save_config.prefix")
        prefix + requestId + "/"
    }
    private def zipFileAbsolutePath(requestId: String): String = {
        val tmpFolderPath = AppConf.getConfig("data_exhaust.save_config.local_path")
        tmpFolderPath + requestId + ".zip";
    }

    /**
     * package all the data according to the request and return the response
     * @param jobRequest request for data-exhaust
     * @return Response for the request
     */
    def packageExhaustData(jobRequest: JobRequest)(implicit sc: SparkContext) {

        val requestId = jobRequest.request_id;
        val clientKey = jobRequest.client_key;
        val inputEventsCount = jobRequest.input_events.getOrElse(0L)
        val outputEventsCount = jobRequest.output_events.getOrElse(0L)
        println("Processing package for request:", requestId);

        val jobId = UUID.randomUUID().toString();
        val exhaustExeTime = jobRequest.execution_time.getOrElse(0).asInstanceOf[Number].longValue();
        try {
            val time = CommonUtil.time({
                val saveType = AppConf.getConfig("data_exhaust.save_config.save_type")
                downloadSourceData(saveType, requestId);
                deleteInvalidFiles(requestDataLocalPath(requestId));
                val data = getEventsData(requestDataLocalPath(requestId));

                val metadata = generateManifestFile(data, jobRequest, requestDataLocalPath(requestId))
                generateDataFiles(data, jobRequest, requestDataLocalPath(requestId))
                CommonUtil.zipDir(zipFileAbsolutePath(requestId), requestDataLocalPath(requestId))
                CommonUtil.deleteDirectory(requestDataLocalPath(requestId));
                uploadZipFile(saveType, requestId, clientKey);
            });

            val packageExeTime = time._1;
            val fileStats = time._2;

            val fieCreatedDate = new DateTime(fileStats._2.get("lastModified").getOrElse(0L).asInstanceOf[Long]);
            val fileExpiryDate = fieCreatedDate.plusDays(30);
            val filePath = fileStats._1;
            val fileSize = fileStats._2.getOrElse("size", 0).asInstanceOf[Number].longValue();

            val completedDate = DateTime.now(DateTimeZone.UTC);
            val result = DataExhaustPackage(requestId, clientKey, jobId, exhaustExeTime + packageExeTime, "COMPLETED", 0L, Option(completedDate), Option(filePath), Option(fileSize), Option(fieCreatedDate), Option(fileExpiryDate));
            sc.makeRDD(Seq(result)).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "job_id", "execution_time", "status", "latency", "dt_job_completed", "location", "file_size", "dt_file_created", "dt_expiration"));
            // JOB_END event with success
            JobLogger.end("DataExhaust Job Completed for "+requestId, "SUCCESS", Option(Map("tag" -> clientKey, "inputEvents" -> inputEventsCount, "outputEvents" -> outputEventsCount, "timeTaken" -> Double.box((exhaustExeTime + packageExeTime) / 1000), "SubmittedDate" -> jobRequest.dt_job_submitted.toString(), "channel" -> "in.ekstep")), "org.ekstep.analytics", requestId);
        } catch {
            case t: Throwable =>
                t.printStackTrace();
                val result = DataExhaustPackage(requestId, clientKey, jobId, exhaustExeTime, "FAILED");
                sc.makeRDD(Seq(result)).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "job_id", "execution_time", "status", "latency"));
                // JOB_END event with failure
                JobLogger.end("DataExhaust Job Failed for "+requestId, "FAILED", Option(Map("tag" -> clientKey, "inputEvents" -> inputEventsCount, "outputEvents" -> 0, "timeTaken" -> Double.box(exhaustExeTime / 1000), "SubmittedDate" -> jobRequest.dt_job_submitted.toString(), "channel" -> "in.ekstep")), "org.ekstep.analytics", requestId);
        }
    }

    /**
     * Delete all invalid files in a given file directory
     * @param path directory path
     */
    private def deleteInvalidFiles(path: String) {
        val fileObj = new File(path);
        fileObj.listFiles.map { x => if (x.isDirectory()) deleteInvalidFiles(x.getAbsolutePath); x }
            .filter { x => invalidFileFilter(x.getAbsolutePath) }
            .map { x => x.delete() };
    }

    /**
     * Generate manifest json file and save into the request folder struture.
     * @param data Array of all merge data with respect to each event identifier
     * @param jobRequest request for data-exhaust
     * @param requestDataLocalPath path of the events file
     * @return manifest case class
     */
	def generateManifestFile(data: Array[EventData], jobRequest: JobRequest, requestDataLocalPath: String)(implicit sc: SparkContext): ManifestFile = {
		val requestData = JSONUtils.deserialize[RequestConfig](jobRequest.request_data)
		val fileExtenstion = requestData.output_format.getOrElse("json").toLowerCase()
	    val fileInfo = data.map { f =>
	        //val synctsRDD = f.data.map { x => DataExhaustUtils.stringToObject(x, requestData.dataset_id.get.toString())._1 }
	        val count = if("csv".equals(fileExtenstion)) f.data.count - 1 else f.data.count
			FileInfo("data/" + f.eventName + "." + fileExtenstion, count)
		}
		//val events = sc.union(data.map { event => event.data }.toSeq).sortBy { x => JSONUtils.deserialize[Map[String, AnyRef]](x).get("ets").get.asInstanceOf[Number].longValue() }
		val totalEventCount = jobRequest.output_events.get
		val firstEventDate = jobRequest.dt_first_event.get.toString();
		val lastEventDate = jobRequest.dt_last_event.get.toString();
		val manifest = ManifestFile("ekstep.analytics.dataset", "1.0", new Date().getTime, jobRequest.request_id, requestData.dataset_id.get.toString(), totalEventCount, firstEventDate, lastEventDate, fileInfo, JSONUtils.serialize(requestData))
		val gson = new GsonBuilder().setPrettyPrinting().create();
		val jsonString = gson.toJson(manifest)
		val outputManifestPath = requestDataLocalPath + "manifest.json"
		OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> outputManifestPath)), Array(jsonString));
		manifest
	}

	/**
     * Generate manifest json file and save into the request folder struture.
     * @param data Array of all merge data with respect to each event identifier
     * @param jobRequest request for data-exhaust
     * @param requestDataLocalPath path of the manifest file
     */
    def generateDataFiles(data: Array[EventData], jobRequest: JobRequest, requestDataLocalPath: String)(implicit sc: SparkContext) {
        val fileExtenstion = JSONUtils.deserialize[RequestConfig](jobRequest.request_data).output_format.getOrElse("json").toLowerCase()
        data.foreach { events =>
            val fileName = events.eventName
            val fileData = events.data
            val path = requestDataLocalPath + "data/" + fileName + "." + fileExtenstion
            OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> path)), fileData);
            CommonUtil.deleteDirectory(requestDataLocalPath + fileName)
        }
    }
}