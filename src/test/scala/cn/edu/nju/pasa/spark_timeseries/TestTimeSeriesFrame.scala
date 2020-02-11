package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestTimeSeriesFrame extends FunSuite with BeforeAndAfterAll {
  private var ss:SparkSession = _


  override def beforeAll(): Unit = {
    ss = SparkSession.builder().appName("local_test")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    ss.close()
  }

  test("test TimeSeriesFrame") {
    val data = Array(
      (0L, "A", 1.2),
      (3L, "B", 3.7),
      (3L, "A", 4.2),
      (1L, "C", 1.4),
      (2L, "A", 12.2),
      (0L, "C", 1.9),
      (1L, "A", 1.2),
      (0L, "B", 2.7),
      (2L, "B", 3.6),
      (3L, "C", 1.2),
      (1L, "B", 2.2),
      (2L, "C", 2.2))

    val rawDataDF = ss.createDataFrame(data).toDF("timestamp", "key", "value")

    val tsf = new TimeSeriesFrame(rawDataDF, ss)

    assert(tsf.getTimeSeriesDF().count() == 3)
    assert(tsf.getTimeSeriesByFeatureName("A") == Vectors.dense(1.2, 1.2, 12.2, 4.2))
  }
}
