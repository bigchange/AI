package com.bigchange.util

import java.io.{File, FileOutputStream}

import jxl.Workbook
import jxl.write.{Label, WritableSheet, WritableWorkbook}
import org.apache.poi.xssf.usermodel.{XSSFSheet, XSSFWorkbook}
import org.apache.spark.rdd.RDD

/**
  * Created by C.J.YOU on 2017/1/11.
  * excel operation
  */
class ExcelWriter(rootDir: String, fileName: String)  extends  Serializable {

  private var writableBook: WritableWorkbook = null

  private  val xssfwb = new XSSFWorkbook()

  /**
    * 初始化sheet，设定header的行位置position 和 header 内容
    * @param sheet WritableSheet sheet 类型
    * @param position 位置
    * @param header 标题
    */
  def sheetInit(sheet:WritableSheet, position: Int, header: Array[String]) = {

    header.zipWithIndex.foreach { case(labelText, index) =>
      val label = new Label(index, position, labelText) // 列，行
      sheet.addCell(label)
    }

  }

  /**
    * 初始化sheet，设定header的行位置position 和 标题内容
    * @param sheet XSSFSheet sheet 类型
    * @param position 位置
    * @param header 标题
    */
  def sheetRowInit(sheet: XSSFSheet, position: Int, header: Array[String]): Unit =  {

    val row = sheet.createRow(position)

     header.zipWithIndex.foreach { case(label, index) =>
       row.createCell(index).setCellValue(label)
     }

  }

  // 根据指定数据格式填充sheet的方法有待实现


  /**
    * 写入数据为xlsx文件, row限制13万
    * @param data 数据内容
    */
  def  createXLSX(data: RDD[String]) = {

    val os = new FileOutputStream(rootDir + "/test.xlsx", true)

    var i = 1

    data.foreach { x =>

      val sheet = xssfwb.createSheet("sheet1")

      sheetRowInit(sheet, 0, Array("stock","time","value"))

      // create first sheet and for each row filled with specific value
      val row = sheet.createRow(i)
      row.createCell(0).setCellValue(x)
      i += 1

      xssfwb.write(os)
      os.close()

    }

  }

  /**
    * 写入数据到xls文件， row 限制在65536
    * @param data 数据内容
    */
  def createXLS(data: RDD[(String,Iterable[((String, String),Int)])]) = {

    var sheetNumber = 0

    data.sortBy(_._1).foreach { x =>

      val path = x._1.split("-").slice(0, 2).mkString("-")  // 按月

      val file = new File(rootDir + "/" + path + ".xls")

      if(!file.exists()) {

        file.createNewFile()
        writableBook = Workbook.createWorkbook(new File(rootDir + "/" + fileName))
        sheetNumber = 0

      } else {

        writableBook = Workbook.createWorkbook(file, Workbook.getWorkbook(file))
        val  numberOfSheet = writableBook.getNumberOfSheets
        sheetNumber = numberOfSheet

      }

      val writerData = x._2
      println("writerData:" + writerData.size)

      var sheet = writableBook.createSheet(x._1, sheetNumber)

      // 初始化新sheet的模板
      sheetInit(sheet, 0, Array("stock","time","value"))

      var i = 1
      var k = 0 // 超出行限制另建sheet
      writerData.foreach { key =>

        if(i > 60000) {
          sheetNumber += 1
          k = k + 1
          writableBook.createSheet(x._1 + "-" + k, sheetNumber)
          sheet = writableBook.getSheet(sheetNumber)
          // 初始化新sheet的模板
          sheetInit(sheet, 0, Array("stock","time","value"))
          i = 0
        }
        val stockName = new Label(0, i, key._1._1)
        val time = new Label(1, i, key._1._2)
        val number = new Label(2, i, key._2.toString)
        sheet.addCell(stockName)
        sheet.addCell(time)
        sheet.addCell(number)
        i += 1
      }
      writableBook.write()
      writableBook.close()
      sheetNumber += 1

    }
  }
}

