package cn.edu.nju.pasa.spark_timeseries

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ArrayUtilsTest extends FunSuite with BeforeAndAfterEach {

  private val array = Array(1.0, 2, 3, 4, 5)

  override def beforeEach() {

  }

  test("testRollLeft") {
    assert(ArrayUtils.roll(array, 2) sameElements Array(3, 4, 5, 1, 2))
  }

  test("testRollRight") {
    val result = ArrayUtils.roll(array, -2)
    assert(result sameElements Array(4, 5, 1, 2, 3))
  }

  test("testDot") {
    val lhs = Array(2, 4.0, 1.5, 5)
    val rhs = Array(1.0, 3, 2, 5.5)
    val res = ArrayUtils.dot(lhs, rhs)
    assert(res == 44.5)
  }

  test("testSum") {
    assert(ArrayUtils.sum(array) == 15)
  }

  test("testMean") {
    assert(ArrayUtils.mean(array) == 3)
  }

  test("testSqrt") {
    val result = ArrayUtils.sqrt(
      Array(1.0, 4.0, 9.0, 16, 25)
    )
    assert(result sameElements array)
  }

  test("testNormalize") {
    val result = ArrayUtils.normalize(array)
    assert(result sameElements Array(-2.0, -1, 0, 1, 2.0).map(_ / math.sqrt(2)))
  }

  test("testWhere") {
    val result = ArrayUtils.where(array, _ > 2)
    assert(result sameElements Array(false, false, true, true, true))
  }

  test("testAbs") {
    val origin = Array(1.0, -2.0, -3.0, 4.0, -5)
    assert(ArrayUtils.abs(origin) sameElements array)
  }

  test("testStd") {
    assert(ArrayUtils.std(array) == math.sqrt(2.0))
  }

  test("testDiff") {
    val result = ArrayUtils.diff(array)
    assert(result sameElements Array(1, 1, 1, 1, 1))
  }

}
