package org.ekstep.analytics.dashboard

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.{AlgoInput, AlgoOutput, FrameworkContext, IBatchModelTemplate, Output}
import org.ekstep.analytics.model.ComptencyInput

case class CompetencyInput() extends AlgoInput
@scala.beans.BeanInfo
case class CompetencyComparisonOutput() extends Output with AlgoOutput

object CompetencyComparisonModel extends IBatchModelTemplate[String, ComptencyInput, CompetencyComparisonOutput, CompetencyComparisonOutput] with Serializable {

  override def preProcess(events: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[ComptencyInput] = {
    sc.parallelize(Seq())
  }

  override def algorithm(events: RDD[ComptencyInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CompetencyComparisonOutput] = {
    val requestBody = """{"operationName": "filterCompetencies","variables": { "cod": "","competencyType": "","competencyArea": ""}, "query": "query filterCompetencies($cod: String, $competencyType: String, $competencyArea: String) {  getAllCompetencies(    cod: $cod    competencyType: $competencyType    competencyArea: $competencyArea  ) {    name    id    description    status    source    additionalProperties {      competencyType      competencyArea      __typename    }    __typename  }}"}"""
    val post = new HttpPost("https://frac-dictionary-backend.igot-stage.in/graphql")
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(requestBody))
    val response = (new DefaultHttpClient).execute(post)
    val re = response.getEntity
    val result = EntityUtils.toString(response.getEntity)
    val result1 = result.map {f=> CompetencyComparisonOutput}
  }

  override def postProcess(events: RDD[CompetencyComparisonOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CompetencyComparisonOutput] = {
    events
  }
}
