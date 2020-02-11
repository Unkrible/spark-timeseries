package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.mllib.linalg.{Vector, Vectors}

object TimeSeriesUtils {

  def roll(x: Vector, shift: Int): Vector = {
    val idx = shift % x.size
    val array = x.toArray
    Vectors.dense(array.takeRight(x.size - idx) ++ array.take(idx))
  }

  def estimateFriedrichCoef(x: Vector, m: Int, r: Float): Vector = {
    Vectors.dense(0)
  }
}
