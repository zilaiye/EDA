package com.kylin.skywork.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object DataTransform {

  def row2col(iter: Iterator[org.apache.spark.sql.Row],ncols:Int) : Iterator[(Int,Array[Double])]= {
    var resArray=new Array[List[Double]](ncols)
    (0 until ncols).map(index => resArray(index) = List[Double]())
    while (iter.hasNext) {
      val row = iter.next
      (0 until row.length).map{
        index =>
          if( !row.isNullAt(index) ) {
            val value = row.getAs[Double](index)
            resArray(index)+:= value
          }
      }
    }
    (0 until resArray.length).zip(resArray).map( v => { val index = v._1
      val arr = v._2.toArray
      //quickSort(arr)
      (index,arr)}).filter(x => x._2.length > 0).iterator
  }


  // 列转行，方便并行操作，生成rdd，这个操作会去掉无用的空数据
  // df 待处理的DataFrame
  // isCache 是否缓存这个数据
  def cols_2_rows(df:DataFrame,isCache:Boolean=true):RDD[(Int,Array[Double])] = {
    val ncols = df.schema.fieldNames.length
    val rdd = df.rdd.mapPartitions(iter => row2col(iter,ncols))
    if(isCache)  rdd.cache()
    rdd
  }

}
