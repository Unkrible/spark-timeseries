package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.mllib.linalg.{Vector, Vectors}

object TimeSeriesUtils {

  def roll(x: Vector, shift: Int): Vector = {
    Vectors.dense(ArrayUtils.roll(x.toArray, shift))
  }

  def diff(x: Vector, derivative: Int=1): Vector = {
    Vectors.dense(ArrayUtils.diff(x.toArray, derivative))
  }

  def estimateFriedrichCoef(x: Vector, m: Int, r: Float): Vector = {
    // TODO: complete it
    Vectors.dense(0)
  }

  def aggregateOnChunks(x: Vector, aggFunc: Array[Double] => Double, chunkSize: Int): Vector = {
    val res = x.toArray.sliding(chunkSize, chunkSize).map(aggFunc).toArray
    Vectors.dense(res)
  }

  def aggAutoCorrelation(x: Vector, maxLag: Int, aggFunc: Array[Double] => Double): Vector = {
    // TODO: complete it
    Vectors.dense(0)
  }

  def partialAutoCorrelation(x: Vector): Vector = {
    // TODO: complete it
    Vectors.dense(0)
  }

  def cidCE(x: Vector, normalize: Boolean): Vector = {
    val array = ArrayUtils.diff(
      if (normalize) ArrayUtils.normalize(x.toArray)
      else x.toArray
    )
    Vectors.dense(ArrayUtils.sqrt(ArrayUtils.dot(array, array)))
  }

  def meanChange(x: Vector): Vector = {
    Vectors.dense(ArrayUtils.mean(ArrayUtils.diff(x.toArray)))
  }

  def meanSecondDerivativeCentral(x: Vector): Vector = {
    val arrayX = x.toArray
    val rollLeft = ArrayUtils.roll(arrayX, 1)
    val rollRight = ArrayUtils.roll(arrayX, -1)
    Vectors.dense(ArrayUtils.mean(
      arrayX.zip(rollLeft.zip(rollRight)).map(each => {
        val (a, (b, c)) = each
        (a - 2 * b + c)/ 2.0
      })
    ))
  }

  def absoluteSumOfChanges(x: Vector): Vector = {
    Vectors.dense(ArrayUtils.sum(
      ArrayUtils.abs(ArrayUtils.diff(x.toArray))
    ))
  }

  def longestStrikeAboveMean(x: Vector): Vector = {
    val arrayX = x.toArray
    val mean = ArrayUtils.mean(arrayX)
    Vectors.dense(ArrayUtils.seqLengthWhere(
      ArrayUtils.where(arrayX, ele => ele > mean)
    ).max)
  }

  def numberPeaks(x: Vector): Vector = {
    // TODO: complete it
    x
  }

  def numberCWTPeaks(x: Vector): Vector = {
    // TODO: complete it
    x
  }

  def timeReversalAsymmetryStatistic(x: Vector, lag: Int): Vector = {
    val arrayX = x.toArray
    val num = arrayX.length
    Vectors.dense(
      if (2 * lag >= num) {
        0.0
      } else {
        val lagOne = ArrayUtils.roll(arrayX, -lag)
        val lagTwo = ArrayUtils.roll(arrayX, - 2 * lag)
        ArrayUtils.mean(
          arrayX.zip(lagOne.zip(lagTwo)).map(ele => {
            val (cur, (one, two)) = ele
            two * two * one - one * cur * cur
          }).slice(0, num - 2 * lag)
        )
      }
    )
  }

  def c3(x: Vector, lag: Int): Vector = {
    val arrayX = x.toArray
    val num = arrayX.length
    Vectors.dense(
      if (2 * lag > num) {
        0.0
      } else {
        val lagOne = ArrayUtils.roll(arrayX, -lag)
        val lagTwo = ArrayUtils.roll(arrayX, - 2 * lag)
        ArrayUtils.mean(
          arrayX.zip(lagOne.zip(lagTwo)).map(ele => {
            val (cur, (one, two)) = ele
            two * one * cur
          }).slice(0, num - 2 * lag)
        )
      }
    )
  }

  def autoCorrelation(x: Vector, lag: Int): Vector = {
    // TODO: complete it
    x
  }
}
