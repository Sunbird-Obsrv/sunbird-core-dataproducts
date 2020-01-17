package org.ekstep.media.common

/**
  * Common Methods for Media Service Result
  *
  * @author gauraw
  */
trait Result {

  def getSubmitJobResult(response: MediaResponse): Map[String, AnyRef]

  def getJobResult(response: MediaResponse): Map[String, AnyRef]

  def getCancelJobResult(response: MediaResponse): Map[String, AnyRef]

  def getListJobResult(response: MediaResponse): Map[String, AnyRef]
}
