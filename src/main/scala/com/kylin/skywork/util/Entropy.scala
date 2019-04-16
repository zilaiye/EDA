package com.kylin.skywork.util

import org.apache.spark.rdd.RDD

object Entropy {

  def _entropy(array:Array[Int],base:Double=scala.math.E):Double = {
    val sum = array.reduce(_+_)
    var entropy_sum = array.map(e => e*1.0/sum).map(e=> -scala.math.log(e)*e).reduce(_+_)
    if( base != scala.math.E ){
      entropy_sum /= scala.math.log(base)
    }
    entropy_sum
  }

  // 计算熵，需要行数及log的底
  def entropy(iter:Iterable[Array[Double]],length:Int,base:Double=scala.math.E):(Int,Double,Double) = {
    var kvMap = Map[Double,Int]()
    var non_missing = 0
    iter.foreach(one => { one.foreach(oneDouble => kvMap += (oneDouble -> (kvMap.getOrElse(oneDouble,0) + 1)))
      non_missing += one.length
    })
    val value_cnt = kvMap.values.size
    val entropy = _entropy(kvMap.values.toArray,base)
    if(length > non_missing ){
      kvMap += (Double.NaN -> (length - non_missing ))
    }
    val entropy_nan = _entropy(kvMap.values.toArray,base)
    (value_cnt,entropy,entropy_nan)
  }

  // 计算熵  compute_entropy
  def compute_entropy(rdd:RDD[(Int,Array[Double])],total_count:Long):Map[Int,(Int,Double,Double)] = {
    val count_map = rdd.map(e=> (e._1,e._2.length)).reduceByKey(_+_).collect().toMap
    rdd.groupByKey(512).map(v => {
      val index = v._1
      val length = count_map.get(index).get
      val entropy_value = entropy(v._2,total_count.toInt,scala.math.E)
      (index,entropy_value)
    }).collect().toMap
  }

}
