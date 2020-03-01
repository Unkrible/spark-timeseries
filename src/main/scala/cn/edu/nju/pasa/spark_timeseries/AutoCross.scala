package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression

class AutoCross(val ss: SparkSession, val timeSeriesDF: TimeSeriesFrame) {
  val featureSets = collection.mutable.Set.empty[Feature]

  def searchFeature(maxFeatures: Int): Unit = {
    val features: Array[Feature] = timeSeriesDF.getFeaturesNames.map(new Feature(_))
    val ops = TimeSeriesUtils.getOpList
    var candidateFeatures = features
    println("User the following ops:")
    ops.foreach(println)

    // all single feature
    featureSets.clear()
    features.foreach(featureSets.add)

    for (_ <- 1 to maxFeatures) {
      // generate candidates
      val lastDF = timeSeriesDF.getTimeSeriesDfByFeatures(candidateFeatures)
      val newFeaWithVec = lastDF.rdd.flatMap { r =>
        val feat = r.getAs[String](0)
        val vec = r.getAs[Vector](1)
        val label = r.getAs[Vector](2)
        ops.map((feat, _, vec, label))
      }.map(x =>
        {
          val (feat, opName, vec, label) = x
          val (name, ops) = Feature.parseFeat(feat)
          val newOps = ops ++ List(opName)
          (Feature.getFeatName(name, newOps), TimeSeriesUtils.doOp(opName, vec), label)
        }
      ).cache()

      // get candidates
      val candidates = newFeaWithVec.map(_._1).collect().map(new Feature(_))
      // add new df to time series df
      val tempRdd = newFeaWithVec
      val df = ss.createDataFrame(tempRdd).toDF("feature", "vector", "label")
      timeSeriesDF.addTimeSeriesDF(df)

      // evaluate
      val chosenFea = evaluateCandidates(candidates)
      chosenFea.foreach(featureSets.add)
      candidateFeatures = chosenFea
    }
  }

  private def evaluateCandidates(candidates: Array[Feature], k: Int=1): Array[Feature] = {
    val candidateNames = candidates.map(_.toString)
    val prevFeatDf = timeSeriesDF.getTimeSeriesDfByFeatures(featureSets.toArray)
    val curFeatDf = timeSeriesDF.getTimeSeriesDfByFeatures(candidates)

    val allFeatDF = prevFeatDf.union(curFeatDf)
    val training = TimeSeriesFrame.makeFeatureDF(allFeatDF, ss)

    val lr = new LinearRegression()
      .setRegParam(0.4)

    val model = lr.fit(training)

    // get candidates' score and sort them
    val featWithScores = allFeatDF.select("feature").collect().map(_.getString(0))
      .zip(model.coefficients.toArray.map(math.abs))
      .filter(x => candidateNames.contains(x._1))
      .sortBy(_._2)(Ordering.Double.reverse)

    print("======" + featWithScores.mkString(" "))

    // get top k feature
    val topk = if (k > featWithScores.length) featWithScores.length else k

    (0 until topk).toArray.map(featWithScores(_)).map(x => new Feature(x._1))
  }
}
