package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class TimeSeriesFrame(val rawDataDF: DataFrame, val ss: SparkSession) {
  private var timeSeriesDF = makeTimeSeriesDF(rawDataDF)

  def getTimeSeriesDF: DataFrame = {
    timeSeriesDF
  }

  /***
    * Translate raw data to column-based data frame
    * @param rawDataDF (0: datetime, 1: key, 2: value)
    * @return column-based data
    */
  def makeTimeSeriesDF(rawDataDF: DataFrame): DataFrame = {
    TimeSeriesFrame.makeTimeSeriesDF(rawDataDF, ss)
  }

  /***
    * Translate timerSeriesDF to row-based data for training of LR
    * @param timeSeriesDF (0: key, 1: value, 2: label)
    * @return row-based data
    */
  def makeFeatureDF(timeSeriesDF: DataFrame): DataFrame = {
    TimeSeriesFrame.makeFeatureDF(timeSeriesDF, ss)
  }

  /***
    * Get all feature names
    * @return list of feature names
    */
  def getFeaturesNames: Array[String] = {
    val names = timeSeriesDF.select("feature").collect().map(_.getString(0))

    names
  }

  /***
    * Union df with time series data frame
    * @param df data frame to union
    */
  def addTimeSeriesDF(df: DataFrame): Unit = {
    timeSeriesDF = timeSeriesDF.union(df).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def getTimeSeriesVecByFeatureName(name: String): Vector = {
    val vec = timeSeriesDF.where("feature = '" + name + "'")
      .select("value").collect()
      .last.getAs[Vector](0)

    vec
  }

  def getTimeSeriesDfByFeature(fea: Feature): DataFrame = {
    val name = fea.toString
    val df = timeSeriesDF.where("feature = '" + name + "'")

    df
  }

  /***
    * Get column-based data frame
    * @param feas names of feature needed
    * @return column-based df in (feature name, vector, ops) format
    */
  def getTimeSeriesDfByFeatures(feas: Array[Feature]): DataFrame = {
    val feaNames = feas.map(_.toString)

    val df = timeSeriesDF.filter(x => feaNames.contains(x.getString(0)))

    df
  }
}


object TimeSeriesFrame {
  /***
    * Translate raw data to column-based data frame
    * @param rawDataDF (0: datetime, 1: key, 2: value, 3: label)
    * @return column-based data
    */
  def makeTimeSeriesDF(rawDataDF: DataFrame, ss: SparkSession): DataFrame = {
    // original data
    // (0: Datetime, 1: Key, 2: Value, 3: Label)
    val timeseriesRdd = rawDataDF.rdd
      .map(x => (x.getString(1), List((x.getLong(0), x.getDouble(2), x.getDouble(3)))))
      .reduceByKey((x, y) => x ++ y)
      .map(x => (x._1, x._2.sortBy(_._1)))
      .map(x => (x._1, Vectors.dense(x._2.map(_._2).toArray), Vectors.dense(x._2.map(_._3).toArray)))

    // column-based data
    val timeseriesDF = ss.createDataFrame(timeseriesRdd).toDF("feature", "value", "label")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    timeseriesDF
  }

  /***
    * Translate time series data to row-based data frame
    * @param timeSeriesDF (feature=name+ops: String, value: Vector, label: Vector)
    * @param ss: spark session
    * @return row-based data for ML
    */
  def makeFeatureDF(timeSeriesDF: DataFrame, ss: SparkSession): DataFrame = {
    val vecLabelRdd = timeSeriesDF.select("value", "label").rdd
    val trainRDD = vecLabelRdd.zipWithIndex().flatMap { case (r, rowIdx) =>
      val vec = r.getAs[Vector](0).toArray
      val label = r.getAs[Vector](1).toArray
      vec.zipWithIndex.map { case (v, colIdx) =>
        (colIdx, List((rowIdx, v, label(colIdx))))
      }
    }.reduceByKey((x, y) => x ++ y).map(x => x._2.sortBy(_._1)).map{r =>
      val feas = Vectors.dense(r.map(_._2).toArray)
      val label = r.last._3
      (label, feas)
    }

    val df = ss.createDataFrame(trainRDD).toDF("label", "features")

    df
  }
}
