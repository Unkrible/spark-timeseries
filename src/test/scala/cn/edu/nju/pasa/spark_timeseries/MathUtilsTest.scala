package cn.edu.nju.pasa.spark_timeseries

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class MathUtilsTest extends FunSuite with BeforeAndAfterEach {

  private val array = Array(1.0, 2, 3, 4, 5)

  override def beforeEach() {
  }

  override def afterEach() {

  }

  test("testSum") {
    assert(ArrayUtils.sum(array) == 15)
  }

  test("testMean") {
    assert(ArrayUtils.mean(array) == 3)
  }

  test("testStd") {
    assert(ArrayUtils.std(array) == 2.0)
  }

}
