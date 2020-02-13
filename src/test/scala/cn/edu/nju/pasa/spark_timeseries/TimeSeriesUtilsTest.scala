package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class TimeSeriesUtilsTest extends FunSuite with BeforeAndAfterEach {

  private val vec = Vectors.dense(1.0, 2, 3, 4)

  override def beforeEach() {

  }

  test("testRoll") {
    val rollX = TimeSeriesUtils.roll(vec, 2).toArray
    assert(rollX sameElements Array(3.0, 4.0, 1.0, 2.0))
  }

  test("testRollOutOfRange") {
    val rollX = TimeSeriesUtils.roll(vec, 6).toArray
    assert(rollX sameElements Array(3.0, 4.0, 1.0, 2.0))
  }

  test("testAggregateOnChunks") {
    val res = TimeSeriesUtils.aggregateOnChunks(vec, _.foldLeft(0.0)((lhs, rhs) => lhs + rhs),2)
    assert(res.toArray sameElements Array(3.0, 7.0))
  }

  test("testCidCE") {
    val res = TimeSeriesUtils.cidCE(vec, true)
    res.toArray.foreach(println)
  }

}
