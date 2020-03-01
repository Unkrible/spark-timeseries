package cn.edu.nju.pasa.spark_timeseries

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.default.parallelism", "500")
      .set("spark.speculation", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ss = SparkSession
      .builder()
      .appName("Time Series")
//      .master("local")
      .config(conf)
      .getOrCreate()
    val dataset =
      (0 until args(0).toInt).map(i => (i.toLong, "GOOG", math.random, math.random)) ++
        (0 until args(0).toInt).map(i => (i.toLong, "AAPL", math.random, math.random)) ++
        (0 until args(0).toInt).map(i => (i.toLong, "MSFT", math.random, math.random))
    val df = ss.createDataFrame(dataset).toDF("timestamp", "key", "value", "label")
    df.show()
    val tsFrame = new TimeSeriesFrame(df, ss)
    tsFrame.getTimeSeriesDF.show
    val autoCross = new AutoCross(ss, tsFrame)
    autoCross.searchFeature(args(1).toInt)
  }
}
