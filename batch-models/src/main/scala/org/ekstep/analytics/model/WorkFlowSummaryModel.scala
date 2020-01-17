package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework._

case class WorkflowInput(sessionKey: WorkflowIndex, events: Buffer[V3Event]) extends AlgoInput
case class WorkflowOutput(index: WorkflowIndex, summaries: Buffer[MeasuredEvent]) extends AlgoOutput
case class WorkflowIndex(did: String, channel: String, pdataId: String)

object WorkFlowSummaryModel extends IBatchModelTemplate[V3Event, WorkflowInput, MeasuredEvent, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.WorkFlowSummaryModel"
    override def name: String = "WorkFlowSummaryModel"
    val serverEvents = Array("LOG", "AUDIT", "SEARCH");

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[WorkflowInput] = {

        val defaultPDataId = V3PData(AppConf.getConfig("default.consumption.app.id"), Option("1.0"))
        val parallelization = config.getOrElse("parallelization", 20).asInstanceOf[Int];
        val partitionedData = data.filter(f => !serverEvents.contains(f.eid)).map { x => (WorkflowIndex(x.context.did.getOrElse(""), x.context.channel, x.context.pdata.getOrElse(defaultPDataId).id), Buffer(x)) }
            .partitionBy(new HashPartitioner(parallelization))
            .reduceByKey((a, b) => a ++ b);
        
        partitionedData.map { x => WorkflowInput(x._1, x._2) }
            
    }
    
    override def algorithm(data: RDD[WorkflowInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[MeasuredEvent] = {
        
        
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];
        val sessionBreakTime = config.getOrElse("sessionBreakTime", 30).asInstanceOf[Int];

        data.map({ f =>
            var summEvents: Buffer[MeasuredEvent] = Buffer();
            val sortedEvents = f.events.sortBy { x => x.ets }
            var rootSummary: org.ekstep.analytics.util.Summary = null
            var currSummary: org.ekstep.analytics.util.Summary = null
            var prevEvent: V3Event = sortedEvents.head
            
            sortedEvents.foreach{ x =>

                val diff = CommonUtil.getTimeDiff(prevEvent.ets, x.ets).get
                if(diff > (sessionBreakTime * 60) && !StringUtils.equalsIgnoreCase("app", x.edata.`type`)) {
                    if(currSummary != null && !currSummary.isClosed){
                        val clonedRootSummary = currSummary.deepClone()
                        clonedRootSummary.close(summEvents, config)
                        summEvents ++= clonedRootSummary.summaryEvents
                        clonedRootSummary.clearAll()
                        rootSummary = clonedRootSummary
                        currSummary.clearSummary()//clonedRootSummary.getLeafSummary
                    }
                }
                prevEvent = x
                (x.eid) match {

                    case ("START") =>
                        if (rootSummary == null || rootSummary.isClosed) {
                            if ((StringUtils.equalsIgnoreCase("START", x.eid) && !StringUtils.equalsIgnoreCase("app", x.edata.`type`))) {
                                rootSummary = new org.ekstep.analytics.util.Summary(x)
                                rootSummary.updateType("app")
                                rootSummary.resetMode()
                                currSummary = new org.ekstep.analytics.util.Summary(x)
                                rootSummary.addChild(currSummary)
                                currSummary.addParent(rootSummary, idleTime)
                            }
                            else {
                                if(currSummary != null && !currSummary.isClosed){
                                    currSummary.close(summEvents, config);
                                    summEvents ++= currSummary.summaryEvents;
                                }
                                rootSummary = new org.ekstep.analytics.util.Summary(x)
                                currSummary = rootSummary
                            } 
                        }
                        else if (currSummary == null || currSummary.isClosed) {
                            currSummary = new org.ekstep.analytics.util.Summary(x)
                            if (!currSummary.checkSimilarity(rootSummary)) rootSummary.addChild(currSummary)
                        }
                        else {
                            val tempSummary = currSummary.checkStart(x.edata.`type`, Option(x.edata.mode), currSummary.summaryEvents, config)
                            if (tempSummary == null) {
                                val newSumm = new org.ekstep.analytics.util.Summary(x)
                                if (!currSummary.isClosed) {
                                    currSummary.addChild(newSumm)
                                    newSumm.addParent(currSummary, idleTime)
                                }
                                currSummary = newSumm
                            }
                            else {
                                if(tempSummary.PARENT != null && tempSummary.isClosed) {
                                     summEvents ++= tempSummary.summaryEvents
                                     val newSumm = new org.ekstep.analytics.util.Summary(x)
                                     if (!currSummary.isClosed) {
                                        currSummary.addChild(newSumm)
                                        newSumm.addParent(currSummary, idleTime)
                                     }
                                     currSummary = newSumm
                                     //currSummary = new org.ekstep.analytics.util.Summary(x)
                                     tempSummary.PARENT.addChild(currSummary)
                                     currSummary.addParent(tempSummary.PARENT, idleTime)
                                }
                                else {
                                  if (currSummary.PARENT != null) {
                                    summEvents ++= currSummary.PARENT.summaryEvents
                                  }
                                  else {
                                    summEvents ++= currSummary.summaryEvents
                                  }
                                  currSummary = new org.ekstep.analytics.util.Summary(x)
                                  if(rootSummary.isClosed) {
                                    summEvents ++= rootSummary.summaryEvents
                                    rootSummary = currSummary
                                  }
                              }
                            }
                        }
                    case ("END") =>
                        // Check if first event is END event, currSummary = null
                        if(currSummary != null && (currSummary.checkForSimilarSTART(x.edata.`type`, if(x.edata.mode == null) "" else x.edata.mode))) {
                            val parentSummary = currSummary.checkEnd(x, idleTime, config)
                            if(currSummary.PARENT != null && parentSummary.checkSimilarity(currSummary.PARENT)) {
                                if (!currSummary.isClosed) {
                                    currSummary.add(x, idleTime)
                                    currSummary.close(summEvents, config);
                                    summEvents ++= currSummary.summaryEvents
                                    currSummary = parentSummary
                                }
                            }
                            else if(parentSummary.checkSimilarity(rootSummary)) {
                                val similarEndSummary = currSummary.getSimilarEndSummary(x)
                                if(similarEndSummary.checkSimilarity(rootSummary)) {
                                    rootSummary.add(x, idleTime)
                                    rootSummary.close(rootSummary.summaryEvents, config)
                                    summEvents ++= rootSummary.summaryEvents
                                    currSummary = rootSummary
                                }
                                else {
                                    if (!similarEndSummary.isClosed) {
                                        similarEndSummary.add(x, idleTime)
                                        similarEndSummary.close(summEvents, config);
                                        summEvents ++= similarEndSummary.summaryEvents
                                        currSummary = parentSummary
                                    }
                                }
                            }
                            else {
                              if (!currSummary.isClosed) {
                                currSummary.add(x, idleTime)
                                currSummary.close(summEvents, config);
                                summEvents ++= currSummary.summaryEvents
                              }
                              currSummary = parentSummary
                            }
                        }
                    case _ =>
                        if (currSummary != null && !currSummary.isClosed) {
                            currSummary.add(x, idleTime)
                        }
                        else{
                            currSummary = new org.ekstep.analytics.util.Summary(x)
                            currSummary.updateType("app")
                            if(rootSummary == null || rootSummary.isClosed) rootSummary = currSummary
                        }
                }
            }
            if(currSummary != null && !currSummary.isClosed){
                currSummary.close(currSummary.summaryEvents, config)
                summEvents ++= currSummary.summaryEvents
                if(rootSummary != null && !currSummary.checkSimilarity(rootSummary) && !rootSummary.isClosed){
                    rootSummary.close(rootSummary.summaryEvents, config)
                    summEvents ++= rootSummary.summaryEvents
                }
            }
            summEvents;
        }).flatMap(f => f.map(f => f));
        
    }
    override def postProcess(data: RDD[MeasuredEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[MeasuredEvent] = {
        data.distinct()
    }
}