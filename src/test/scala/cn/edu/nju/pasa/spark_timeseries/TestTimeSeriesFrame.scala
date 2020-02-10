package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TestTimeSeriesFrame extends FunSuite with BeforeAndAfterAll {
  private var spark:SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("udf testings")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.close()
  }

  test("test TimeSeriesFrame") {

  }
}
