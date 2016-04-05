package scala.telecom.classification

/**
  * Created by C.J.YOU on 2016/3/28.
  */
object Gov extends UrlType {

  private  val url ="gov"
  override val urlType: String = "gov"

  def classify(string: String): Boolean ={
    if(string.contains(url)) true else false
  }
}
