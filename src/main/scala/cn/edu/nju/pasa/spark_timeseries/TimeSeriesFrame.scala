package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class TimeSeriesFrame(val rawDataDF: DataFrame, val ss: SparkSession) {
  private val timeSeriesDF = TimeSeriesFrame.getTimeSeriesDF(rawDataDF, ss)

  def getTimeSeriesDF(): DataFrame = {
    timeSeriesDF
  }

  def getTimeSeriesByFeatureName(name: String): Vector = {
    val vec = timeSeriesDF.where("feature = '" + name + "'")
      .select("vector").collect()
      .last.getAs[Vector](0)

    vec
  }
}

object TimeSeriesFrame {
  def getTimeSeriesDF(rawDataDF: DataFrame, ss:SparkSession): DataFrame = {
    val timeseriesRdd = rawDataDF.rdd
      .map(x => (x.getString(1), List((x.getLong(0), x.getDouble(2)))))
      .reduceByKey((x, y) => x ++ y)
      .map(x => (x._1, x._2.sortBy(_._1)))
      .map(x => (x._1, Vectors.dense(x._2.map(_._2).toArray)))

    val timeseriesDF = ss.createDataFrame(timeseriesRdd).toDF("feature", "vector")
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    timeseriesDF
  }
}
