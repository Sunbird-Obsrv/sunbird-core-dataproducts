package org.ekstep.analytics.util

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.ekstep.analytics.framework.util.JobLogger

class HDFSFileUtils(classNameStr: String, jobLogger: JobLogger.type ) {
  implicit val className = classNameStr

  def recursiveListFiles(file: File, ext: String): Array[File] = {
    val fileList = file.listFiles
    val extOnly = fileList.filter(file => file.getName.endsWith(ext))
    extOnly ++ fileList.filter(_.isDirectory).flatMap(recursiveListFiles(_, ext))
  }

  def purgeDirectory(dir: File): Unit = {
    for (file <- dir.listFiles) {
      if (file.isDirectory) purgeDirectory(file)
      file.delete
    }
  }

  def purgeDirectory(dirName: String): Unit = {
    val dir = new File(dirName)
    purgeDirectory(dir)
  }
  
  // Gets first level sub-directories
  def getSubdirectories(dirName: String): List[File] = {
      val file = new File(dirName)
      if (file.exists && file.isDirectory) {
          val files = file.listFiles.filter(_.isDirectory).toList
          files
      } else if (file.exists && file.isFile) {
        List[File](file)
      } else {
          List[File]();
      }
  }
  
  def renameDirectory(oldName: String, newName: String) {
    val oldDir = new File(oldName)
    if (oldDir.isDirectory()) {
      oldDir.renameTo(new File(newName))
      jobLogger.log(s"Renamed $oldName to $newName")
      println(s"Renamed $oldName to $newName")
    }
  }

  def renameReport(tempDir: String, outDir: String, fileExt: String, fileNameSuffix: String = null) = {
    val regex = """\=.*/""".r // example path "somepath/partitionFieldName=12313144/part-0000.csv"
    val temp = new File(tempDir)
    val out = new File(outDir)

    if (!temp.exists()) throw new Exception(s"path $tempDir doesn't exist")

    if (!out.exists()) {
      out.mkdirs()
      jobLogger.log(s"creating the directory ${out.getPath}")
    }

    val fileList = recursiveListFiles(temp, fileExt)

    jobLogger.log(s"moving ${fileList.length} files to ${out.getPath}")

    fileList.foreach(file => {
      val value = regex.findFirstIn(file.getPath).getOrElse("")
      if (value.length > 1) {
        val partitionFieldName = value.substring(1, value.length() - 1)
        val filePath = new File(s"${out.getPath}/$partitionFieldName/$fileNameSuffix$fileExt")
        if (!filePath.exists()) {
          filePath.mkdirs();
        }

        Files.copy(file.toPath, filePath.toPath(), StandardCopyOption.REPLACE_EXISTING)
        jobLogger.log(s"${partitionFieldName} Copied from ${file.toPath.toAbsolutePath()} to ${filePath}" )

      }
    })
  }


  def copyFilesToDir(files: Array[File], dirName: String) {
    files.foreach(file => {
      val newPath = new File(dirName+"/"+file.getName)
      Files.copy(file.toPath, newPath.toPath, StandardCopyOption.REPLACE_EXISTING)
    })
  }

}
