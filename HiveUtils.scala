package com.katchstyle.analytics.pipelines

import org.apache.spark.sql.{DataFrame,SparkSession}

object HiveUtils {
def readDatabase(ingestDate: String, database: String, table: String)(implicit spark: SparkSession): DataFrame = {
spark.sql(s"select * from ${database}_${ingestDate}.${table}")

}
