package com.kylin.skywork.util

import org.apache.spark.rdd.RDD

object PSI {


  def scale_range(input:Array[Double], min:Double, max:Double):Array[Double] = {
    val input_min = input.min
    val input_max = input.max
    val output = input.map(_ - input_min).map(x => x*1.0*(max-min)/input_max + min ).toArray
    output
  }

  def get_buckets(element:Double,bins:Array[Double]):Int = {
    bins.foldLeft(-1)((a,x)=> { if(element >= x) {
      scala.math.min(a+1,bins.length-2)
    }else{
      a
    }})
  }

  def sub_psi(e_perc:Double,a_perc:Double):Double = {
    var c_e = e_perc
    var c_a = a_perc
    if( e_perc == 0 ){
      c_e = 0.0001D
    }
    if( a_perc == 0 ){
      c_a = 0.0001D
    }
    (c_e - c_a) * scala.math.log(c_e / c_a)
  }

  //获取每个列的分割方式，如果是按照分箱数分箱的话
  def get_breaks_buckets(rdd:RDD[(Int,Array[Double])],buckets:Int):Map[Int,(Array[Double],Int)] = {
    val min_max_len_map = rdd.map(e=>(e._1,(e._2.min,e._2.max,e._2.length)))
      .reduceByKey((a,b)=>
        (scala.math.min(a._1,b._1),
          scala.math.max(a._2,b._2),a._3+b._3))
      .collect().toMap
    val breakPoints = (0 until buckets+1).map(e=> e*1.0/10*100).toArray
    min_max_len_map.map(x => {
      val index = x._1
      val (min,max,len) = x._2
      val bins = scale_range(breakPoints,min,max)
      (index,(bins,len))
    })
  }

  def get_bins_new( e:Double , arr:Array[Double]):Int = {
    val len = arr.length
    if( (len <= 0) ||  (e > arr(len-1)) || (e < arr(0)) ){
      -1
    }else{
      get_buckets(e,arr)
    }
  }

  def histogram(iter:Iterable[Array[Double]],bins:Array[Double]):Array[Int] = {
    val len = bins.length - 1
    if( len < 0 ) {
      Array[Int](0)
    }else{
      val counts = new Array[Int](len)
      iter.foreach(one => one.map( e => get_bins_new(e,bins)).filter(_ != -1)
        .foreach( i => counts(i) = counts(i) + 1 ))
      counts
    }
  }

  //获取每个列的分割方式，如果是按照分箱数分箱的话
  def get_breaks_quantiles(rdd:RDD[(Int,Array[Double])],buckets:Int):Map[Int,(Array[Double],Int)] = {
    val min_max_len_map = rdd.map(e=>(e._1,(e._2.min,e._2.max,e._2.length)))
      .reduceByKey((a,b)=>
        (scala.math.min(a._1,b._1),
          scala.math.max(a._2,b._2),a._3+b._3))
      .collect().toMap
    val breakPoints = (0 until buckets+1).map(e=> e*1.0/10).toArray
    val quantile_values_map = rdd.groupByKey(512).map(e => {
      val index = e._1
      val iter = e._2
//      val length = min_max_len_map.getOrElse(index,(Double.NaN,Double.NaN,0))._3
      val (min,max,length) = min_max_len_map.getOrElse(index,(Double.NaN,Double.NaN,0))
      val bins = breakPoints.map(p => {
        if( p == 0 ) min
        else if ( p== 1 ) max
        else   Quantiles.percentile(iter,p,length)
      }).toArray
      (index,bins)
    }).collect().toMap

    min_max_len_map.map(x => {
      val index = x._1
      val (min,max,len) = x._2
      val bins = quantile_values_map.getOrElse(index,Array[Double]()).distinct.sorted
      (index,(bins,len))
    })
  }


  def calculate_psi_rdd(rdd1:RDD[(Int,Array[Double])],rdd2:RDD[(Int,Array[Double])],ncols:Int, buckettype:String = "bins", buckets:Int = 10 ):Array[Double] = {
    val test_len_map = rdd2.map(e=>(e._1,e._2.length)).reduceByKey(_+_).collect().toMap
    var breaks_lens_map:Map[Int,(Array[Double],Int)] = Map[Int,(Array[Double],Int)]()
    var result = Array[Double]()
    if(buckettype == "bins") {
      breaks_lens_map = get_breaks_buckets(rdd1,buckets)
    }else {
      breaks_lens_map = get_breaks_quantiles(rdd1,buckets)
    }
    val m1_1 = rdd1.groupByKey(512).map(v => {
      val index = v._1
      val bins = breaks_lens_map.getOrElse(index,(Array[Double](),0))._1
      (index,histogram(v._2,bins))
    }).collect().sortBy(_._1)
    val m2_1 = rdd2.groupByKey(512).map(v => {
      val index = v._1
      val bins = breaks_lens_map.getOrElse(index,(Array[Double](),0))._1
      (index,histogram(v._2,bins))
    }).collect().sortBy(_._1)
    val m1_map = m1_1.toMap
    val m2_map = m2_1.toMap
    result = (0 until ncols).map(i => {val a1 = m1_map.getOrElse(i,Array[Int]())
      val a2 = m2_map.getOrElse(i,Array[Int]())
      val sum1 = breaks_lens_map.getOrElse(i,(Array[Double](),0))._2
      val sum2 = test_len_map.getOrElse(i,0)
      val merge_arr = a1.zipAll(a2,0,0)
      val merge_arr2 = merge_arr.map( x => {
        var x_1:Double = x._1
        var x_2:Double = x._2
        if(sum1 == 0 ){
          x_1 = 0
        } else {
          x_1 = x_1*1.0/sum1
        }
        if(sum2 == 0 ){
          x_2 = 0
        } else {
          x_2 = x._2*1.0/sum2
        }
        (x_1,x_2)
      })
      merge_arr2.map(x => sub_psi(x._1,x._2)).toArray.sum
    }).toArray
    result
  }

}
