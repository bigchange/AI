package com.bigchange.basic


/**
  * Created by C.J.YOU on 2016/12/6.
  */
//
trait  PolyFunction[R] {

  def apply[T](t: T): R

  type P[U]

}


object Parameter extends PolyFunction[String]{

  def apply[T](p: T) =  p.toString

}

object Test {

  def main(args: Array[String]) {

    println(Parameter(2).getClass)

  }
}


