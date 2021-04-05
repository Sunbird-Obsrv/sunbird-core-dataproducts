package org.ekstep.analytics.util

import java.sql.{ResultSet, Statement}

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import java.sql.Connection

object EmbeddedPostgresql {

  var pg: EmbeddedPostgres = null;
  var connection: Connection = null;
  var stmt: Statement = null;

  def start() {
    pg = EmbeddedPostgres.builder().setPort(65124).start()
    connection = pg.getPostgresDatabase().getConnection()
    stmt = connection.createStatement()
  }

  def createDeviceProfileTable(): Boolean = {
    val tableName: String = Constants.DEVICE_PROFILE_TABLE
    val query = s"""
                   |CREATE TABLE IF NOT EXISTS $tableName (
                   |    device_id TEXT PRIMARY KEY,
                   |    api_last_updated_on TIMESTAMP,
                   |    avg_ts float,
                   |    city TEXT,
                   |    country TEXT,
                   |    country_code TEXT,
                   |    device_spec json,
                   |    district_custom TEXT,
                   |    fcm_token TEXT,
                   |    first_access TIMESTAMP,
                   |    last_access TIMESTAMP,
                   |    producer_id TEXT,
                   |    state TEXT,
                   |    state_code TEXT,
                   |    state_code_custom TEXT,
                   |    state_custom TEXT,
                   |    total_launches bigint,
                   |    total_ts float,
                   |    uaspec json,
                   |    updated_date TIMESTAMP,
                   |    user_declared_district TEXT,
                   |    user_declared_state TEXT,
                   |    user_declared_on TIMESTAMP)""".stripMargin

    execute(query)
  }

  def createExperimentTable(): Boolean = {
    val tableName: String = Constants.EXPERIMENT_DEFINITION_TABLE
    val query =
        s"""
           |CREATE TABLE IF NOT EXISTS $tableName (
           |exp_id VARCHAR(50),
           |created_by VARCHAR(50),
           |created_on TIMESTAMP,
           |criteria VARCHAR(100),
           |exp_data VARCHAR(300),
           |exp_description VARCHAR(200),
           |exp_name VARCHAR(50),
           |stats VARCHAR(300),
           |status VARCHAR(50),
           |status_message VARCHAR(50),
           |updated_by VARCHAR(50),
           |updated_on TIMESTAMP,
           |PRIMARY KEY(exp_id));
         """.stripMargin
    execute(query)
  }

  def createReportConfigTable(): Boolean = {
    val tableName: String = Constants.DRUID_REPORT_CONFIGS_DEFINITION_TABLE
    val query=
      s"""CREATE TABLE IF NOT EXISTS $tableName(
         |report_id text,
         |updated_on timestamptz,
         |report_description text,
         |requested_by text,
         |report_schedule text,
         |config json,
         |created_on timestamptz,
         |submitted_on timestamptz,
         |status text,
         |status_msg text,
         |batch_number int,
         |PRIMARY KEY(report_id))""".stripMargin
         execute(query)
  }

  def execute(sqlString: String): Boolean = {
    stmt.execute(sqlString)
  }

  def executeQuery(sqlString: String): ResultSet = {
    stmt.executeQuery(sqlString)
  }

  def close() {
    stmt.close()
    connection.close()
    pg.close()
  }
}
