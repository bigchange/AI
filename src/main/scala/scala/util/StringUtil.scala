package scala.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.SecureRandom
import java.util.zip.{GZIPInputStream, DeflaterOutputStream, InflaterOutputStream}
import javax.crypto.{SecretKeyFactory, Cipher}
import javax.crypto.spec.DESKeySpec

import org.json.JSONObject
import sun.misc.BASE64Decoder

/**
  * Created by C.J.YOU on 2016/3/17.
  */
object StringUtil {

  /**
    * 使用gzip进行压缩
    */
  private  def zlibZip(primStr:String):String = {
    if (primStr == null || primStr.length () == 0) {
      return primStr
    }
    val out = new ByteArrayOutputStream ()
    val gzip = new DeflaterOutputStream (out)
    try {
      gzip.write (primStr.getBytes())
    } catch {
      case e: Exception => e.printStackTrace ();
    } finally {
      if (gzip != null) {
        try {
          gzip.close ()
        } catch {
          case e: Exception => e.printStackTrace ();
        }
      }
    }
     new sun.misc.BASE64Encoder().encode(out.toByteArray)
  }


  /**
    *
    * <p>Description:使用gzip进行解压缩</p>
    */
  private def  zlibUnzip(compressedStr:String ):String ={
    if(compressedStr == null){
      return null
    }
    val bos = new ByteArrayOutputStream()
    val  zos = new InflaterOutputStream(bos)
    try{
      zos.write(new sun.misc.BASE64Decoder().decodeBuffer(compressedStr))
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if(zos !=null ){
        zos.close()
      }
      if(bos !=null){
        bos.close()
      }
    }
     new String(bos.toByteArray)
  }

  def transform(string: String): String ={
    zlibZip(string)
  }

  def unTransform(string: String): String ={
    zlibUnzip(string)
  }

  private def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  // 电信数据加码后对应的解码 方法
  private def design(str:String): String ={
    val cipher = Cipher.getInstance ("des")
    val keySpec = new DESKeySpec ("kunyandata".getBytes ())
    val keyFactory = SecretKeyFactory.getInstance ("des")
    val secretKey = keyFactory.generateSecret (keySpec)
    cipher.init(Cipher.DECRYPT_MODE, secretKey, new SecureRandom())
    val plainData = cipher.doFinal(new BASE64Decoder().decodeBuffer(str))
    decodeBase64(new String(plainData))
  }

  private def getJsonObject(line:String): JSONObject = {
    val data = new JSONObject(line)
    data
  }

  def parseJsonObject(str:String): (String,String,String,String) ={
    val json = getJsonObject(str)
    val id = json.get("id").toString
    val value = json.get("value").toString
    val desDe = design(value)
    println(desDe)
    val resultJson = getJsonObject(desDe)
    val url = resultJson.get("third").toString
    val ref = resultJson.get("fourth").toString
    val cookie  = resultJson.get("seventh").toString
    println(s"url=$url, refer:$ref, cookie=$cookie")
    (id,url,ref,cookie)
  }


  /**
    * <p>Description:使用gzip进行解压缩</p>
    * @param compressedStr
    * @return
    */
  def  gunzip(compressedStr:String ):String ={
    if(compressedStr == null){
      return null
    }
    var decompressed =""
    val out = new ByteArrayOutputStream()
    val compressed = new sun.misc.BASE64Decoder().decodeBuffer(compressedStr)
    val in = new ByteArrayInputStream(compressed)
    val ginzip = new GZIPInputStream(in)
    try {
      val buffer = new Array[Byte](1024)
      var offset = ginzip.read(buffer)
      while (offset != -1) {
        out.write(buffer,0,offset)
        offset = ginzip.read(buffer)
      }
      decompressed = out.toString()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if(ginzip !=null ){
        ginzip.close()
      }
      if(in !=null){
        in.close()
      }
      if(out !=null){
        out.close()
      }
    }
    decompressed
  }

  def main(args: Array[String]): Unit = {

  }

}
