package scala.telecom.classification

/**
  * Created by C.J.YOU on 2016/3/28.
  */
object TakeOut extends UrlType {

  override val urlType: String = "takeout"
  private  val url = ",pailequ,beequick,180.97.93.28,4008823823,meituan,"

  def classify(string: String): Boolean ={
    val targetUrl  = string.replace(".com","").replace(".cn","").replace("www.","").replace("http://","").replace("https://","").split("/")(0)
    if(url.contains(","+targetUrl+","))  true else false
  }

}
