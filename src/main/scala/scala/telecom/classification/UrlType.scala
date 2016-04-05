package scala.telecom.classification

/**
  * Created by C.J.YOU on 2016/3/28.
  */
trait UrlType {

  val urlType = ""

}
object UrlType {
  def apply(string: String): UrlType = {
    if(Car.classify(string)){
      Car
    }else if(Gov.classify(string)){
      Gov
    }else if(Luxury.classify(string)){
      Luxury
    }else if(Stock.classify(string)){
      Stock
    }else if(Weibo.classify(string)){
      Weibo
    } else if (TakeOut.classify(string)){
      TakeOut
    }else  None
  }
}
