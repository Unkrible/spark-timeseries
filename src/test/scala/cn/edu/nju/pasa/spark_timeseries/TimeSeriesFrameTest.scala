package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TimeSeriesFrameTest extends FunSuite with BeforeAndAfterAll {
  private var ss: SparkSession = _


  override def beforeAll(): Unit = {
    ss = SparkSession.builder().appName("local_test")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    ss.close()
  }

  test("test create TimeSeriesFrame") {

//    val training = ss.read.format("libsvm")
//      .load("/Users/chengfeng/Work/scala/spark/data/mllib/sample_linear_regression_data.txt")

    val data = Array(
      (0L, "A", 1.2, 2.21),
      (3L, "B", 3.7, 2.3),
      (3L, "A", 4.2, 2.3),
      (1L, "C", 1.4, 4.3),
      (2L, "A", 12.2, 5.6),
      (0L, "C", 1.9, 6.7),
      (1L, "A", 1.2, 3.4),
      (0L, "B", 2.2, 9.2),
      (2L, "B", 3.6, 2.3),
      (3L, "C", 1.2, 4.2),
      (1L, "B", 2.2, 6.1),
      (2L, "C", 2.2, 9.8))

    val rawDataDF = ss.createDataFrame(data).toDF("timestamp", "key", "value", "label")

    val tsf = new TimeSeriesFrame(rawDataDF, ss)

    assert(tsf.getTimeSeriesDF.count() == 3)
    assert(tsf.getTimeSeriesVecByFeatureName("A") == Vectors.dense(1.2, 1.2, 12.2, 4.2))
  }

  test("test makeFeatureDF") {

    val data = Array(
      (0L, "A", 1.2, 2.21),
      (3L, "B", 3.7, 2.3),
      (3L, "A", 4.2, 2.3),
      (1L, "C", 1.4, 4.3),
      (2L, "A", 12.2, 5.6),
      (0L, "C", 1.9, 6.7),
      (1L, "A", 1.2, 3.4),
      (0L, "B", 2.2, 9.2),
      (2L, "B", 3.6, 2.3),
      (3L, "C", 1.2, 4.2),
      (1L, "B", 2.2, 6.1),
      (2L, "C", 2.2, 9.8))

    val rawDataDF = ss.createDataFrame(data).toDF("timestamp", "key", "value", "label")
    val tsf = new TimeSeriesFrame(rawDataDF, ss)
    val tsfdf = tsf.getTimeSeriesDF

    val trainDF = tsf.makeFeatureDF(tsfdf)

    assert(trainDF.count() == 4)
    assert(trainDF.columns.contains("features"))
    assert(trainDF.columns.contains("label"))
  }
}
