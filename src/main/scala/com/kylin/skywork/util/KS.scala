package com.kylin.skywork.util

object KS {
  // """
  // 计算KS值函数
  // :param result: 模型预测结果,包括n_sample的数组
  // :param yte: 标准答案，包括n_sample的数组
  // :param threshold: 分割阈值，-1表示自动选择最优阈值，否则以输入阈值来分割
  // :return: threshold阈值
  // """
  def get_ks(result:Array[Double],yte:Array[Double],threshold:Double = -1.0D):(Double,Double) = {
    var result_y = result.zip(yte).sorted
    val pos = yte.sum
    val neg = yte.length - pos
    var max_ks = 0.0D
    var pcur = 0.0D
    var ncur = 0.0D
    val yte_len = yte.length
    var threshold_ = threshold
    if( threshold == -1 ){
      val ks = new Array[Double](yte_len)
      (0 until yte_len).foreach(row => {
        if(result_y(row)._2 == 1){
          pcur += 1
        }else{
          ncur += 1
        }
        if( (row + 1 < yte_len)  && (Math.abs(result_y(row+1)._1 - result_y(row)._1) < 1e-15) ) {}
        else {
          ks(row) = Math.abs(1.0 * pcur / pos - 1.0 * ncur / neg )
          if( ks(row)  > max_ks ){
            max_ks = ks(row)
            threshold_ = result_y(row)._1
          }
        }
      })
    } else {
      result_y.filter(_._1 <= threshold).foreach(r => {
        if( r._2 == 1) pcur += 1
        else  ncur += 1
      })
      max_ks = Math.abs(1.0 * pcur / pos - 1.0 * ncur / neg)
    }
    (max_ks,threshold_)
  }
}
