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

  private val globalArray = Array(
    "finance.china.com.cn","finance.sina.com.cn","caijing.com.cn","ce.cn","17ok.com","jrj.com.cn","finance.qq.com","eastmoney.com","caixin.com",
    "money.163.com","finance.ifeng.com","business.sohu.com","xueqiu.com","10jqka.com.cn","cnfol.com","hexun.com","simuwang.com","howbuy.com","licai.com",
    "go-goal.com","touzi.com","zhongguocaifu.com.cn","jfz.com","simu.eastmoney.com","cailianpress.com","live.sina.com.cn/zt/f/v/finance/globalnews1","gov.cn",
    "mfa.gov.cn","mod.gov.cn","ndrc.gov.cn","moe.gov.cn","most.gov.cn","miit.gov.cn","seac.gov.cn","mps.gov.cn","ccdi.gov.cn","mca.gov.cn","moj.gov.cn",
    "mof.gov.cn","mohrss.gov.cn","mlr.gov.cn","mep.gov.cn","mohurd.gov.cn","moc.gov.cn","mwr.gov.cn","moa.gov.cn","mofcom.gov.cn","mcprc.gov.cn","nhfpc.gov.cn",
    "pbc.gov.cn","audit.gov.cn","china-language.gov.cn","cnsa.gov.cn","caea.gov.cn","sasac.gov.cn","customs.gov.cn","chinatax.gov.cn","saic.gov.cn",
    "aqsiq.gov.cn","国家新闻出版广电总局.com","sport.gov.cn","chinasafety.gov.cn","sda.gov.cn","stats.gov.cn","forestry.gov.cn","sipo.gov.cn","cnta.gov.cn",
    "sara.gov.cn","counsellor.gov.cn","ggj.gov.cn","nbcp.gov.cn","ncac.gov.cn","gqb.gov.cn","hmo.gov.cn","chinalaw.gov.cn","gwytb.gov.cn","scio.gov.cn",
    "xinhuanet.com","cas.cn","cssn.cn","cae.cn","drc.gov.cn","nsa.gov.cn","cea.gov.cn","cma.gov.cn","cbrc.gov.cn","csrc.gov.cn","circ.gov.cn","ssf.gov.cn",
    "nsfc.gov.cn","gjxfj.gov.cn","chinagrain.gov.cn","nea.gov.cn","sastind.gov.cn","tobacco.gov.cn","safea.gov.cn","scs.gov.cn","soa.gov.cn","sbsm.gov.cn",
    "nra.gov.cn","caac.gov.cn","post.gov.cn","sach.gov.cn","satcm.gov.cn","safe.gov.cn","chinacoal-safety.gov.cn","saac.gov.cn","oscca.gov.cn","beijing.gov.cn",
    "tj.gov.cn","hebei.gov.cn","shanxigov.cn","nmg.gov.cn","ln.gov.cn","jl.gov.cn","hlj.gov.cn","shanghai.gov.cn","jiangsu.gov.cn","zhejiang.gov.cn","ah.gov.cn",
    "fujian.gov.cn","jiangxi.gov.cn","shandong.gov.cn","henan.gov.cn","hubei.gov.cn","hunan.gov.cn","gd.gov.cn","gxzf.gov.cn","hainan.gov.cn","cq.gov.cn",
    "sc.gov.cn","gzgov.gov.cn","yn.gov.cn","xizang.gov.cn","shaanxi.gov.cn","gansu.gov.cn","qh.gov.cn","nx.gov.cn","xinjiang.gov.cn","gov.hk","gov.mo",
    "xjbt.gov.cn","locpg.gov.cn","zlb.gov.cn","people.com.cn","xinhuanet.com","china.com.cn","cctv.com","qstheory.cn","cn.chinadaily.com.cn","cnr.cn","gb.cri.cn",
    "cyol.net","gmw.cn","ce.cn","cnnc.com.cn","cnecc.com","spacechina.com","casic.com.cn","avic.com.cn","cssc.net.cn","csic.com.cn","cngc.com.cn","csgc.com.cn",
    "cetc.com.cn","cnpc.com.cn","sinopecgroup.com","cnooc.com.cn","sgcc.com.cn","csg.cn","chng.com.cn","china-cdt.com","chd.com.cn","cgdc.com.cn","cpicorp.com.cn","ctgpc.com.cn","shenhuagroup.com.cn","chinatelecom.com.cn","chinaunicom.com.cn","chinamobile.com","cec.com.cn","faw.com.cn",
    "dfmc.com.cn","cfhi.com","china-erzhong.com","harbin-electric.com","dongfang.com","ansteelgroup.com","baosteel.com","wisco.com.cn","chalco.com.cn",
    "cosco.com","cnshipping.com","airchinagroup.com","ce-air.com","csair.cn","sinochem.com","cofco.com.cn","minmetals.com.cn","genertec.com.cn","cscec.com",
    "sinograin.com.cn","sdic.com.cn","cmhk.com","crc.com.hk","hkcts.com","snptc.com.cn","comac.cc","cecic.com.cn","ciecc.com.cn","hfjt.com.cn","cctgroup.com.cn",
    "chinacoal.com","ccteg.cn","sinomach.com.cn","cam.com.cn","sinosteel.com","mcc.com.cn","cisri.com.cn","chemchina.com.cn","cncec.cn","sinolight.cn","cnacgc.com",
    "chinasalt.com.cn","chtgc.com","sinoma.cn","cnbm.com.cn","cnmc.com.cn","grinm.com","bgrimm.com","ciic.com.cn","cabr.com.cn","chinacnr.com","csrgc.com.cn","crsc.cn",
    "crecg.com","crcc.cn","ccgrp.com.cn","potevio.com","datanggroup.cn","cnadc.com.cn","chinatex.com","sinotrans-csc.com","chinasilk.com","cfgc.cn","sinopharm.com","citsgroup.com.cn",
    "poly.com.cn","zhzrgs.com.cn","cadreg.com.cn","cmgb.com.cn","ccgc.cn","xxcig.com","travelskyholdings.com","cnaf.com","casc.com.cn","cpecc.net","checc.cn","sinohydro.com","chinagoldgroup.com",
    "cncrc.com.cn","cpgc.cn","luckyfilm.com","cgnpc.com.cn","hualu.com.cn","alcatel-sbell.com.cn","ch.com.cn","wri.com.cn","chinaoct.com","namkwong.com.mo","xd.com.cn","cggc.cn","crmsc.com.cn","crhc.cn",
    "chinacoop.gov.cn","cpasonline.org.cngb","zycg.gov.cn","chinagov.cn","cdpf.org.cn","guba.eastmoney.com","guba.sina.com.cn","finance.china.com.cn","finance.sina.com.cn","caijing.com.cn","ce.cn","17ok.com","jrj.com.cn","finance.qq.com","eastmoney.com",
    "caixin.com","money.163.com","finance.ifeng.com","business.sohu.com","xueqiu.com","10jqka.com.cn","cnfol.com","hexun.com","vipshop.com","vipstore.com","xiu.com","5lux.com","360top.com","shangpin.com","ihaveu.com","secoo.com","glamour-sales.com.cn","zhenpin.com",
    "wbiao.cn","meici.com","cn.strawberrynet.com","luxst.com","vipku.com","jufengshang.com","yaodian100.com","imoda.com","wooha.com","uemall.com","guuoo.com","hongyun2000.com","eushophq.com","epinwei.com","yapinte.com","eogoo.com","mail.163.com","chanel.com","louisvuitton.cn",
    "dior.cn","versace.com","prada.com","sephora.cn","valentino.cn","hugoboss.cn","lesailes.hermes.com","cn.burberry.com","kenzo.com","givenchy.com","cartier.cn","tiffany.cn","bulgari.com","cn.vancleefarpels.com","harrywinston.com","darryring.com","damiani.com","cn.boucheron.com",
    "mikimoto.com.hk","swarovski.com.cn","gucci.com","prada.com","armani.cn","dunhill.cn","fendi.cn","china.coach.com","louisvuitton.com","chanel.com","dior.cn","valentino.cn	","patek.com","audemarspiguet.com","piaget.cn","jaeger-lecoultre.com","vacheron-constantin.com","rolex.com","iwc.com",
    "girard-perregaux.ch","omegawatches.cn","rolls-roycemotorcars.com.cn","bmw.com.cn","chrysler.com.cn","audi.cn","hdmi.org","ford.com.cn","maserati.com.cn","bentleymotors.com","daimler.com","mercedes-benz.com.cn","prada.com","oakley.com.cn",
    "judithleiber.com","donnakaran.com","ysl.com","chanel.com","dior.cn","cartier.cn","louisvuitton.cn","parkerpen.com","montblanc.cn","waterman.com","tw.cartier.com","sheaffer.com","aurorapen.it","cross.com","montegrappa.com","hennessy.com","absolut.com","johnniewalker.com","chivas.com.cn","cn.moet.com",
    "remymartin.com","martell.com","bacardi.com","malts.com","johnniewalker.com.cn","bijan.com","tiffany.cn","opiumbarcelona.com","joythestore.com","chanel.com","hermes.com","dunloptires.com","taylormadegolf.com","nike.com","adidas.com.sg","benhoganmuseum.org","etonic.com","golfsmith.com","callawaygolf.cn","chinapinggolf.com","chanel.com","esteelauder.com.cn","lancome.com.cn","dior.cn","guerlain.com.cn","shiseido.com.hk","china.elizabetharden.com","avon.com.cn","marykay.com","cartier.cn","tiffany.cn","bulgari.com","cn.vancleefarpels.com","harrywinston.com",
    "derier.com.cn","damiani.com","cn.boucheron.com","mikimoto.com.hk","swarovski.com.cn"
  )

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
