package com.kylin.skywork
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.kylin.skywork.util._
object EDA {
  // describe_rdd 用于生成一每一列的数据的
  // rdd 是列转行处理后的rdd
  // total_count是原始dataframe的行数
  def describe_rdd(rdd:RDD[(Int,Array[Double])],total_count:Long,names:Array[String]):
  Array[(String,Double,Double,Double,Int,Double,Double,Double,Double,Double,Double,Double)] = {
    val stats = rdd.map(e => (e._1,Statistics.statstics(e._2)))
      .reduceByKey( (a,b) => (scala.math.min(a._1,b._1),
      scala.math.max(a._2,b._2),
      a._3 + b._3,
      a._4 + b._4
    )).collect().toMap
    //统计missing_rate,avg
    val stats_avg = stats.map(e => ( e._1, e._2._3 * 1.0 / e._2._4 )).toMap
    val stats_var = rdd.map(e => (e._1,Statistics.variance(e._2,stats_avg.get(e._1).get)))
      .reduceByKey(_+_).collect()
      .map(e => { val index = e._1
        val varsum = e._2
        val count = stats.get(index).get._4
        if(count == 1){
          (index,Double.NaN)
        }else{
          (index,varsum*1.0/(count-1))
        }
      }).toMap
    //.mapValues(v=> v*1.0/total_count)
    val count_map = stats.map(e => (e._1,e._2._4))
    val medians = rdd.groupByKey(512).map(v => {
      val index = v._1
      val length = count_map.get(index).get
      val median = Quantiles.getMedian(v._2,0.5,length)
      (index,median)
    }).collect().toMap
    val percent_25 = rdd.groupByKey(512).map(v => {
      val index = v._1
      val length = count_map.get(index).get
      val median = Quantiles.percentile(v._2,0.25,length)
      (index,median)
    }).collect().toMap
    val percent_75 = rdd.groupByKey(512).map(v => {
      val index = v._1
      val length = count_map.get(index).get
      val median = Quantiles.percentile(v._2,0.75,length)
      (index,median)
    }).collect().toMap
    val last_results = (0 until names.length).map( index => {
      val (min,max,sum,count) = stats.getOrElse(index,(Double.NaN,Double.NaN,Double.NaN,0))
      val missing = total_count - count
      var avg = stats_avg.getOrElse(index,Double.NaN)
      //         if (sum.equals(Double.NaN) == false && count != 0 ){
      //             avg = sum*1.0/count
      //         }
      val variance = stats_var.getOrElse(index,Double.NaN)
      var stddev = Double.NaN
      if( variance.equals(Double.NaN) == false){
        stddev = scala.math.sqrt(variance)
      }
      var median = medians.getOrElse(index,Double.NaN)
      val feature_name = names(index)
      val percent25 = percent_25.getOrElse(index,Double.NaN)
      val percent75 = percent_75.getOrElse(index,Double.NaN)
      (feature_name,max,min,sum,count,missing*1.0/total_count,variance,stddev,median,avg,percent25,percent75)
    }).toArray
    last_results
  }
}
