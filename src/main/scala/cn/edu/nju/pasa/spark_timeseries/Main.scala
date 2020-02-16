package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.sql.SparkSession

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("Time Series")
      .master("local")
      .getOrCreate()
    val df = ss.createDataFrame(Seq(
      (5L, "GOOG", 523.0),
      (6L, "GOOG", -1.0),
      (7L, "GOOG", 524.0),
      (8L, "GOOG", 600.0),
      (5L, "AAPL", 384.0),
      (6L, "AAPL", 384.0),
      (7L, "AAPL", 385.0),
      (8L, "AAPL", 385.0),
      (5L, "MSFT", 40.0),
      (6L, "MSFT", 60.0),
      (7L, "MSFT", -1.0),
      (8L, "MSFT", 70.0)
    )).toDF("Timestamp", "Key", "Value")
    df.show()
    val tsFrame = new TimeSeriesFrame(df, ss)
    tsFrame.getTimeSeriesDF.show
    val autoCross = new AutoCross(ss, tsFrame)
    autoCross.getFeatureSet(3)
  }
}
