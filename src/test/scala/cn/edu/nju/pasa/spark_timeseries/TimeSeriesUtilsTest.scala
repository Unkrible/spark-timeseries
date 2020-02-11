package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class TimeSeriesUtilsTest extends FunSuite with BeforeAndAfterEach {

  override def beforeEach() {

  }

  test("testRoll") {
    val x = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val rollX = TimeSeriesUtils.roll(x, 2).toArray
    assert(rollX sameElements Array(3.0, 4.0, 1.0, 2.0))
  }

  test("testRollOutOfRange") {
    val x = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val rollX = TimeSeriesUtils.roll(x, 6).toArray
    assert(rollX sameElements Array(3.0, 4.0, 1.0, 2.0))
  }
}
