package com.kylin.skywork.util

object Quantiles {


  def getMedian(iter:Iterable[Array[Double]],percent:Double,length:Int):Double = {
    val heapsize = (length*percent).toInt + 1
    val queue = new scala.collection.mutable.PriorityQueue[Double]()
    iter.foreach(one => one.foreach(inner_one => {
      if(queue.size < heapsize){
        queue.enqueue(inner_one)
      }else{
        if( inner_one < queue.head ){
          queue.dequeue
          queue.enqueue(inner_one)
        }

      }
    }))
    if( length % 2 == 0 ){
      val m1 = queue.dequeue
      val m2 = queue.dequeue
      (m1+m2)*1.0/2
    }else{
      val m1 = queue.dequeue
      m1
    }
  }

  //分位数的计算的逻辑是有问题的，这个地方需要修正
  //https://wiki.mbalib.com/wiki/%E5%9B%9B%E5%88%86%E4%BD%8D%E6%95%B0
  def percentile(iter:Iterable[Array[Double]],percent:Double,length:Int):Double = {
    var heapsize = 0
    var pos = 0.0D
    var queue:scala.collection.mutable.PriorityQueue[Double] = null
    if( length <= 0 || percent < 0.0D || percent > 1.0D ) Double.NaN
    else {
      if( percent <= 0.5 ){
        //大根堆
        heapsize = ((length-1)*percent).toInt + 2
        if(heapsize > length) heapsize = length
        pos = (length-1)*percent
        queue = new scala.collection.mutable.PriorityQueue[Double]()
        iter.map(one => one.map(inner_one => {
          if(queue.size < heapsize){
            queue.enqueue(inner_one)
          }else{
            if( inner_one < queue.head ){
              queue.dequeue
              queue.enqueue(inner_one)
            }
          }
        }))
      }else{
        //小根堆
        heapsize = ((length-1)*(1-percent)).toInt + 2
        if(heapsize > length) heapsize = length
        pos = (length-1)*(1-percent)
        queue = new scala.collection.mutable.PriorityQueue[Double]()(Ordering[Double].reverse)
        iter.map(one => one.map(inner_one => {
          if(queue.size < heapsize){
            queue.enqueue(inner_one)
          }else{
            if( inner_one > queue.head ){
              queue.dequeue
              queue.enqueue(inner_one)
            }
          }
        }))
      }
      val f = math.floor(pos).toInt
      val g = pos - f
      if( length == 1 ){
        val m1 = queue.dequeue
        m1
      } else if( percent <= 0.5 ){
        val m1 = queue.dequeue
        val m2 = queue.dequeue
        m2 + ( m1 - m2 ) * g
      } else{
        val m1 = queue.dequeue
        val m2 = queue.dequeue
        m1 + ( m2 - m1 ) * ( 1 - g )
      }
    }
  }

  def quantiles(array:Array[Double],cutoffs:Array[Double]):Array[Double] = {
    val len = array.length
    cutoffs.map(cut => {
      val pos1 = len * cut
      val pos2 = pos1.toInt + 1
      val eps:Double = 1e-10
      //说明是个整数
      if( Math.abs( pos1 - Math.floor(pos1) ) < eps ){
        ( array(pos2-1) + array(pos2) )*1.0/2
      }else{
        array(pos2)
      }
    }).toArray
  }

}
