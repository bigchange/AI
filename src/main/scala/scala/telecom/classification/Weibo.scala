package scala.telecom.classification

/**
  * Created by C.J.YOU on 2016/3/28.
  */
object Weibo extends UrlType{

  override val urlType: String = "weibo"
  private  val url =  "weibo"

  def classify(string: String): Boolean ={
    if(string.contains(url)) true else false
  }

}
