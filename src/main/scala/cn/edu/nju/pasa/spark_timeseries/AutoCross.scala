package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression

class AutoCross(val ss: SparkSession, val timeSeriesDF: TimeSeriesFrame) {
  val featureSets = collection.mutable.Set.empty[Feature]

  def getFeatureSet(maxFeatures: Int): Unit = {
    val features: Array[Feature] = timeSeriesDF.getFeaturesNames.map(new Feature(_))
    val ops = TimeSeriesUtils.getOpList
    var candidateFeatures = features

    // all single feature
    featureSets.clear()
    features.foreach(featureSets.add)

    for (_ <- 1 to maxFeatures) {
      // generate candidates
      val lastDF = timeSeriesDF.getTimeSeriesDfByFeatures(candidateFeatures)
      val newFeaWithVec = lastDF.rdd.flatMap { r =>
        val fea = r.getAs[Feature](0)
        val vec = r.getAs[Vector](1)
        ops.map((fea, _, vec))
      }.map(x => executeOp(x._1, x._2, x._3)).cache()

      // get candidates
      val candidates = newFeaWithVec.map(_._1).collect()
      // add new df to time series df
      val tempRdd = newFeaWithVec.map(x => (x._1.toString, x._2))
      val df = ss.createDataFrame(tempRdd).toDF("feature", "vector")
      timeSeriesDF.addTimeSeriesDF(df)

      // evaluate
      val chosenFea = evaluateCandidates(candidates)
      chosenFea.foreach(featureSets.add)
      candidateFeatures = chosenFea
    }
  }

  private def executeOp(fea:Feature, opName: String, x:Vector): (Feature, Vector) = {
    val newOps = fea.ops ++ List(opName)
    val newFea = new Feature(fea.feature, newOps)

    (newFea, TimeSeriesUtils.doOp(opName, x))
  }

  private def evaluateCandidates(candidates: Array[Feature], k: Int=1): Array[Feature] = {
    val prevFeatDf = timeSeriesDF.getTimeSeriesDfByFeatures(featureSets.toArray)
    val curFeatDef = candidates.map(timeSeriesDF.getTimeSeriesDfByFeature)
    // TODO: union all current feature
    val trainDF = prevFeatDf

    val lr = new LinearRegression()
      .setRegParam(0.4)

    val model = lr.fit(trainDF)
    val featWithScores = trainDF.columns.zip(model.coefficients.toArray)

    // get top k feature
    var topk = k
    if (k > featWithScores.length) {
      topk = featWithScores.length
    }

    (0 until topk).toArray.map(featWithScores(_)).map(_._1)
  }
}
