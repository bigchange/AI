package scala.telecom.classification

/**
  * Created by C.J.YOU on 2016/3/28.
  */
object Car extends UrlType{

  private val url = "autohome"
  override val urlType: String = "car"

  def classify(string: String): Boolean ={
    if(string.contains(url)) true else false
  }

}
