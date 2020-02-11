package cn.edu.nju.pasa.spark_timeseries

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class MathUtilsTest extends FunSuite with BeforeAndAfterEach {

  private val array = Array(1.0, 2, 3, 4, 5)

  override def beforeEach() {
  }

  override def afterEach() {

  }

  test("testSum") {
    assert(MathUtils.sum(array) == 15)
  }

  test("testMean") {
    assert(MathUtils.mean(array) == 3)
  }

  test("testStd") {
    assert(MathUtils.std(array) == 2.0)
  }

}
