package com.bigchange.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.SecureRandom
import java.util.zip.{GZIPInputStream, DeflaterOutputStream, InflaterOutputStream}
import javax.crypto.{SecretKeyFactory, Cipher}
import javax.crypto.spec.DESKeySpec

import com.bigchange.log.CLogger
import org.json.JSONObject
import sun.misc.{BASE64Encoder, BASE64Decoder}

/**
  * Created by C.J.YOU on 2016/3/17.
  */
object StringUtil extends  CLogger {

  /**
    * 使用zlibZip进行压缩
    */
  private def zlibZip(primStr:String):String = {

    if (primStr == null || primStr.length () == 0)
      return primStr

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
    * Description:使用zlibZip进行解压缩
    */
  private def zlibUnzip(compressedStr:String ):String = {

    if(compressedStr == null)
      return null

    val bos = new ByteArrayOutputStream()
    val  zos = new InflaterOutputStream(bos)

    try {

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

  /** 外部调用接口 */
  def transform(string: String) = zlibZip(string)

  /** 外部调用接口 */
  def unTransform(string: String) = zlibUnzip(string)


  private def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  // 对应的加码方法
  private def desEncoder(input:String): String = {

    val keySpec = new DESKeySpec ("kunyandata".getBytes ())
    val keyFactory = SecretKeyFactory.getInstance ("des")
    val secretKey = keyFactory.generateSecret (keySpec)
    val plainText = input
    val cipher = Cipher.getInstance ("des")
    cipher.init (Cipher.ENCRYPT_MODE, secretKey, new SecureRandom ())
    val cipherData = cipher.doFinal (plainText.getBytes ())
    val cipherText = new BASE64Encoder().encode(cipherData)

    cipherText

  }

  // 对应的解码方法
  private def design(str:String): String = {

    val cipher = Cipher.getInstance ("des")
    val keySpec = new DESKeySpec ("kunyandata".getBytes ())
    val keyFactory = SecretKeyFactory.getInstance ("des")
    val secretKey = keyFactory.generateSecret (keySpec)
    cipher.init(Cipher.DECRYPT_MODE, secretKey, new SecureRandom())
    val plainData = cipher.doFinal(new BASE64Decoder().decodeBuffer(str))
    // decodeBase64(new String(plainData))
    new String(plainData)
  }

  private def getJsonObject(line:String): JSONObject = {

    val data = new JSONObject(line)

    data

  }

  def parseJsonObject(str:String): String ={

    var result = ""

    try {
      val res = decodeBase64 (str)
      val json = getJsonObject(res)
      val id = json.get ("id").toString
      val value = json.get ("value").toString
      val desDe = design (zlibUnzip (value))
      val resultJson = getJsonObject (desDe)
      val ad = resultJson.get ("1").toString
      val ts = resultJson.get ("2").toString
      val hostAndUrl = resultJson.get ("34").toString
      val ref = resultJson.get ("5").toString
      val ua = resultJson.get ("8").toString
      val cookie = resultJson.get ("9").toString
      result = ts + "\t" + ad + "\t" + ua + "\t" + hostAndUrl + "\t" + ref + "\t" +cookie
    } catch {
      case e:Exception  =>
        error("praseJsonObject ERROR")
    }
    result
  }


  /**
    * Description:使用gzip进行解压缩
    * @param compressedStr
    * @return
    */
  private def gunzip(compressedStr:String ):String = {

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

  /*
   * KMP 字符匹配算法
   */
  private def get_next(t:String, t_len:Int, next:Array[Int]): Unit = {

    var i   = 0
    var  j   = -1
    next(0) = -1

    while( i < t_len) {
      if(j == -1 || t(i) == t(j) )
      {
        i = i + 1
        j = j + 1
        if(i  < t_len)
        {
          if(t(i) != t(j))
            next(i) = j
          else
            next(i) = next(j)
        }
      }
      else
        j = next(j)
    }
  }

  private def index_op(s:String, t:String, pos:Int) : Int = {

    val s_len = s.length
    val t_len = t.length

    if( t_len > s_len )
       return -1

    var next = new Array[Int](t_len)
    next = new Array[Int](t_len)
    get_next(t,t_len,next)

    /** 开始匹配 */
    var i = pos
    var  j = 0
    while(i < s_len && j < t_len )
    {
      if(j == -1 || s(i) == t(j) )
      {
        i = i + 1
        j = j + 1
      }
      else
      {
        j = next(j)
      }
    }
    /** 返回结果 */
    if( j == t_len )
       i - t_len
    else
        -1
  }

}
