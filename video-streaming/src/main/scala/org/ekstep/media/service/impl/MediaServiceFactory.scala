package org.ekstep.media.service.impl

import org.ekstep.media.config.AppConfig
import org.ekstep.media.exception.MediaServiceException
import org.ekstep.media.service.IMediaService

/**
  *
  * @author gauraw
  */
object MediaServiceFactory {

  val SERVICE_TYPE: String = AppConfig.getServiceType()

  def getMediaService(): IMediaService = {
    SERVICE_TYPE.toLowerCase() match {
      case "aws" => AWSMediaServiceImpl
      case "azure" => AzureMediaServiceImpl
      case _ => throw new MediaServiceException("ERR_INVALID_SERVICE_TYPE", "Please Provide Valid Media Service Name")
    }
  }
}
