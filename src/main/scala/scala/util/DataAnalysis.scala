package scala.util

import java.io.ByteArrayOutputStream
import java.security.SecureRandom
import java.util.zip.{DeflaterOutputStream, InflaterOutputStream}
import javax.crypto.spec.DESKeySpec
import javax.crypto.{Cipher, SecretKeyFactory}

import sun.misc.{BASE64Decoder, BASE64Encoder}

import scala.language.postfixOps
import scala.util.control.Breaks._

/**
  * Created by C.J.YOU on 2016/3/7.
  */
object DataAnalysis extends Serializable{

  private val globalArray = Array("")

  private def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  private def encoderBase64(string: String):String ={
    val encoder = new BASE64Encoder().encode(string.getBytes("utf-8"))
    encoder
  }

  private def extractorString(arr:Array[String]): Array[String] ={
    val firstSegment = arr(1)
    val secondSegment = arr(2)
    val thirdSegment = arr(3)
    val fourthSegment = arr(4)
    val fifthSegment = arr(5)
    val eighthSegment = arr(8)
    val ninethSegment = arr(9)
    Array(firstSegment,secondSegment,thirdSegment + fourthSegment,fifthSegment,eighthSegment,ninethSegment)
  }

  private def desEncoder(input:String): String = {
    val keySpec = new DESKeySpec ("kunyandata".getBytes ())
    val keyFactory = SecretKeyFactory.getInstance ("des")
    val secretKey = keyFactory.generateSecret (keySpec)
    val plainText = encoderBase64(input)
    val cipher = Cipher.getInstance ("des")
    cipher.init (Cipher.ENCRYPT_MODE, secretKey, new SecureRandom ())
    val cipherData = cipher.doFinal (plainText.getBytes ())
    val cipherText = new BASE64Encoder().encode(cipherData)
    cipherText
  }

  private def sign(str:String):String ={
    zlibZip(desEncoder(str))
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

  private def toJson(arr: Array[String]): String = {
    val json = "{\"1\":\"%s\", \"2\":\"%s\",\"34\":\"%s\", \"5\":\"%s\", \"8\":\"%s\", \"9\":\"%s\"}"
    json.format(arr(0), arr(1), arr(2), arr(3),arr(4),arr(5))
  }
  private def toJson2(arr: Array[String]): String = {
    val json = "{\"id\":\"%s\", \"value\":\"%s\"}"
    json.format(arr(0), arr(1))
  }

  private def extractorSeg(arr:Array[String]): String ={
    var result = ""
    breakable {
      for(wn <- globalArray) {
        if (arr(2).contains (wn)) {
          println(toJson (arr))
          result = sign (toJson (arr))
          println((design(zlibUnzip(result))))
          break()
        }
      }
    }
    result
  }
  def analysis(str:String): String = {
    val result = extractorString(str.split("\t"))
    val value = extractorSeg(result)
    val arr = Array[String](new Random().nextInt(500).toString,value)
    if(value.nonEmpty){
      toJson2(arr)
    } else null
  }
  private def  zlibZip(primStr:String):String = {
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

 def main(args: Array[String]) {
   val string = "10\t0b5c48ac4d818ef3dc005119982ed18a64a7bc04\t1447895900909\thttp://damiani.com/Hotels-g308272-zfc5-Shanghai-Hotels.html\taHR0cDovL3d3dy5oYnRjLmNjb28uY24vbmV3cy9sb2NhbC80MDI3Mjg0Lmh0bWw\t10\t10\t10\t10\tTW96aWxsYS80LjAgKGNvbXBhdGlibGU7IE1TSUUgNi4wOyBXaW5kb3dzIE5UIDUuMTsgU1YxOyAuTkVUNC4wQzsgLk5FVDQuMEUp\t115.239.210.141\tSE1BQ0NPVU5UPUFFQUNCOUIzMzAzNzlCRjk7IFBTVE09MTQ1MDc2NzgzMjsgQklEVVBTSUQ9QkM0Q0ZCMzAzOTg5NEQ1QUZCMkU5QjQzQzI2QkNBOEI7IEJBSURVSUQ9RTNFRDg4QUU3MkMzOUMzREUwNzYzMjdGQTYwQjBEOTc6Rkc9MTsgQkRTRlJDVklEPVh2QXNKZUNDeEczUm5PYzRZcC1rcWVpeEU4ckJ1TXlTM3RsZzNKOyBIX0JEQ0xDS0lEX1NGPUpKNGVvSUtiZkl2YmZQMGs1UG9FYi1GX2htVDIyLXVzeVRjQ1FoY0gwaE9Xc0lPLWh4bmpqaEZRcWZSREs0Y08zUmJINXQ1ek14ODVmYlJURFVDMC1uRFNISF9PcVRLZTNKOyBITVZUPTIyZWEwMWFmNThiYTJiZTBmZWM3YzExYjI1ZTg4ZTZjfDE0NTc1MDQ3NDd8OyBIX1BTX1BTU0lEPTE4ODgxXzE0NTRfMTgyMDVfMTcwMDFfMTU2NzBfMTIwNjJfMTgwMjBfMTA2MzM="
   println(string.length)
   val result = analysis(string)
   println(result.length)

 }
}
