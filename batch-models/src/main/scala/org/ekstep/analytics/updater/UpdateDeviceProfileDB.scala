package org.ekstep.analytics.updater

import org.apache.spark.rdd._
import org.apache.spark.sql.{Encoders, SQLContext, SaveMode}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants

import scala.collection.mutable.Buffer

case class DeviceProfileKey(device_id: String)
case class DeviceProfileInput(index: DeviceProfileKey, currentData: Buffer[DerivedEvent], previousData: Option[DeviceProfileOutput]) extends AlgoInput

object UpdateDeviceProfileDB extends IBatchModelTemplate[DerivedEvent, DeviceProfileInput, DeviceProfileOutput, Empty] with Serializable {

  val className = "org.ekstep.analytics.model.UpdateDeviceProfileDB"

  override def name: String = "UpdateDeviceProfileDB"

  val db = AppConf.getConfig("postgres.db")
  val url = AppConf.getConfig("postgres.url") + s"$db"
  val connProperties = CommonUtil.getPostgresConnectionProps

  override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DeviceProfileInput] = {
    implicit val sqlContext = new SQLContext(sc)

    val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_DEVICE_SUMMARY")))

    val newGroupedEvents = filteredEvents.map { event =>
      (DeviceProfileKey(event.dimensions.did.get), Buffer(event))
    }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b)

    val encoder = Encoders.product[DeviceProfileOutput]
    val responseRDD = sqlContext.sparkSession.read.jdbc(url, Constants.DEVICE_PROFILE_TABLE, connProperties).as[DeviceProfileOutput](encoder).rdd
    val prevDeviceProfile = responseRDD.map{f => (DeviceProfileKey(f.device_id), f)}
    val deviceData = newGroupedEvents.leftOuterJoin(prevDeviceProfile)
    deviceData.count()
      deviceData.map { x =>
      val deviceProfileInputData = x._2
      DeviceProfileInput(x._1, deviceProfileInputData._1, deviceProfileInputData._2)
    }
  }

  override def algorithm(data: RDD[DeviceProfileInput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[DeviceProfileOutput] = {
    data.map { events =>
      val eventsSortedByFromDate = events.currentData.sortBy { x => x.context.date_range.from }
      val eventsSortedByToDate = events.currentData.sortBy { x => x.context.date_range.to }
      val prevProfileData = events.previousData.getOrElse(DeviceProfileOutput(events.index.device_id, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None))
      val eventStartTime = CommonUtil.getTimestampFromEpoch(eventsSortedByFromDate.head.context.date_range.from)
      val first_access = if (prevProfileData.first_access.isEmpty) eventStartTime else if (eventStartTime.getTime > prevProfileData.first_access.get.getTime) prevProfileData.first_access.get else eventStartTime
      val eventEndTime = CommonUtil.getTimestampFromEpoch(eventsSortedByToDate.last.context.date_range.to)
      val last_access = if (prevProfileData.last_access.isEmpty) eventEndTime else if (eventEndTime.getTime < prevProfileData.last_access.get.getTime) prevProfileData.last_access.get else eventEndTime
      val current_ts = events.currentData.map { x =>
        (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("total_ts").get.asInstanceOf[Double])
      }.sum
      val total_ts = if (prevProfileData.total_ts.isEmpty) current_ts else current_ts + prevProfileData.total_ts.get
      val current_launches = events.currentData.map { x =>
        (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("total_launches").get.asInstanceOf[Number].longValue())
      }.sum
      val total_launches = if (prevProfileData.total_launches.isEmpty) current_launches else current_launches + prevProfileData.total_launches.get
      val avg_ts = if (total_launches == 0) total_ts else CommonUtil.roundDouble(total_ts / total_launches, 2)
      DeviceProfileOutput(events.index.device_id, Option(first_access), Option(last_access), Option(total_ts), Option(total_launches), Option(avg_ts), prevProfileData.device_spec, prevProfileData.uaspec, prevProfileData.state, prevProfileData.city, prevProfileData.country, prevProfileData.country_code, prevProfileData.state_code, prevProfileData.state_custom, prevProfileData.state_code_custom, prevProfileData.district_custom, prevProfileData.fcm_token, prevProfileData.producer_id, prevProfileData.user_declared_state, prevProfileData.user_declared_district, prevProfileData.api_last_updated_on, prevProfileData.user_declared_on)
    }
  }

  override def postProcess(data: RDD[DeviceProfileOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    data.toDF.write.mode(SaveMode.Overwrite).jdbc(url,Constants.DEVICE_PROFILE_TABLE , connProperties)
    sc.makeRDD(List(Empty()));
  }

}