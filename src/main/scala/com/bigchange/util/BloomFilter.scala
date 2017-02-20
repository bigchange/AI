package com.bigchange.util
import java.util

/**
  * Created by C.J.YOU on 2017/1/6.
  * 布隆过滤器：主要用于判断数据是否已存在
  */
class BloomFilter(bitsLength: Int) {

  private  final val DEFAULT_SIZE = 2 << bitsLength

  private  final val seeds = Array(7, 11, 12, 31, 37, 61)

  private  val  bits = new util.BitSet(DEFAULT_SIZE)

  private  val  func = new Array[SimpleHash](seeds.length)

  // 初始化随机种子
  for(i <- seeds.indices) {
    func(i) = new SimpleHash(DEFAULT_SIZE,seeds(i))
  }

  // 添加没有的数据
  def add(value:String)= {

    if(!contain(value)) {
      for(i <- func.indices){
        bits.set(func(i).hash(value),true)
      }
    }
  }

  // 判断是否包含value
  def contain(value:String) : Boolean = {
    if(value == null)
      return false

    var flag = true
    for(i <- func.indices){
      flag = flag && bits.get(func(i).hash(value))
    }
    flag
  }
}
// 取值的简单哈希函数
class SimpleHash(cap:Int,seed:Int) {

  def hash(value:String): Int = {

    var result = 0
    val length = value.length
    for(i <- 0 until length) {
      result = seed * result + value.charAt(i)
    }
    (cap - 1 ) & result
  }
}

object BloomFilter {

  private  var bf : BloomFilter = _

  def apply(length: Int): BloomFilter = {

    if(bf == null) bf = new BloomFilter(length)

    bf

  }

  def getInstance = bf




}
