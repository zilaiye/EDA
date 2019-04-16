package com.kylin.skywork.util

object Statistics {
  def statstics(array:Array[Double]):(Double,Double,Double,Int) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    var sum = 0.0D
    var count = array.length
    array.foreach(e => {
      min = scala.math.min(e,min)
      max = scala.math.max(e,max)
      sum += e
    })
    (min,max,sum,count)
  }

  def variance(array:Array[Double],avg:Double):Double = {
    var variance = 0.0D
    array.foreach(e => variance += scala.math.pow(e - avg,2))
    variance
  }

}
