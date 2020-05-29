package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.Params
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.HTTPClient


case class LocationList(count: Int, response: List[Map[String, String]])

case class LocationResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LocationList)


object DruidQueryUtil {
    def removeInvalidLocations(mainDf: DataFrame, filterDf: DataFrame, columns: List[String])(implicit sc: SparkContext): DataFrame = {
        if (filterDf.count() > 0)
            mainDf.join(filterDf, columns, "inner")
        else {
            mainDf
        }
    }


    def getValidLocations(restUtil: HTTPClient)(implicit sc: SparkContext): DataFrame = {
        implicit val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        val locationUrl = AppConf.getConfig("location.search.url")
        val headers = Map("Authorization" -> AppConf.getConfig("location.search.token"))
        val request =
            s"""
               |{
               |  "request": {
               |    "filters": {},
               |    "limit": 10000
               |  }
               |}
               """.stripMargin
        val response = restUtil.post[LocationResponse](locationUrl, request, Option(headers))
        val masterDf = if (null != response) {
            val states = response.result.response.map(f => {
                if (f.getOrElse("type", "").equalsIgnoreCase("state"))
                    (f.get("id").get, f.get("name").get)
                else
                    ("", "")
            }).filter(f => !f._1.isEmpty)
            val districts = response.result.response.map(f => {
                if (f.getOrElse("type", "").equalsIgnoreCase("district"))
                    (f.get("parentId").get, f.get("name").get)
                else
                    ("", "")
            }).filter(f => !f._1.isEmpty)
            val masterData = states.map(tup1 => districts.filter(tup2 => tup2._1 == tup1._1).map(tup2 => (tup1._2, tup2._2))).flatten.distinct
            sc.parallelize(masterData).toDF("state", "district")
        } else
            sqlContext.emptyDataFrame
        masterDf
    }
}
