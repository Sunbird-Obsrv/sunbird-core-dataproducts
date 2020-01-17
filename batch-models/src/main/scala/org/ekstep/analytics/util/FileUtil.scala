package org.ekstep.analytics.util

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.JobLogger
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

object FileUtil {
  implicit val className = "org.ekstep.analytics.util.FileUtil"

  def renameReport(tempDir: String, outDir: String, batchId: String) = {

    val regex = """\=.*/""".r // to get batchid from the path "somepath/batchid=12313144/part-0000.csv"
    val temp = new File(tempDir)
    val out = new File(outDir)

    if (!temp.exists()) throw new Exception(s"path $tempDir doesn't exist")
    if (out.exists()) {
      purgeDirectory(out)
      JobLogger.log(s"cleaning out the directory ${out.getPath}", None, INFO)
    } else {
      out.mkdirs()
      JobLogger.log(s"creating the directory ${out.getPath}", None, INFO)
    }
    val fileList = recursiveListFiles(temp, ".csv")
    fileList.foreach(file => {
      JobLogger.log("creating a report for " + batchId, None, INFO)
      Files.copy(file.toPath, new File(s"${out.getPath}/report-$batchId.csv").toPath, StandardCopyOption.REPLACE_EXISTING)
    })

  }

  private def recursiveListFiles(file: File, ext: String): Array[File]
  = {
    val fileList = file.listFiles
    val extOnly = fileList.filter(file => file.getName.endsWith(ext))
    extOnly ++ fileList.filter(_.isDirectory).flatMap(recursiveListFiles(_, ext))
  }

  private def purgeDirectory(dir: File): Unit

  = {
    for (file <- dir.listFiles) {
      if (file.isDirectory) purgeDirectory(file)
      file.delete
    }
  }

  def uploadReport(sourcePath: String, provider: String, container: String, objectKey: Option[String]): String = {
    val storageService = StorageServiceFactory
      .getStorageService(StorageConfig(provider, AppConf.getStorageKey(provider), AppConf.getStorageSecret(provider)))
    storageService.upload(container, sourcePath, objectKey.orNull, isDirectory = Option(true), Option(1), None)
  }
}

