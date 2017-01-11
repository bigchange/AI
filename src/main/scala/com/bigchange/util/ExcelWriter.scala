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
object ExcelWriter  extends  Serializable {

  var rootDir = ""

   // first type sheet
  def sheetInit(sheet:WritableSheet) = {

    val label = new Label(0, 0, "股票") // 列，行
    val label1 = new Label(1, 0, "小时")
    val label2 = new Label(2, 0, "查看热度统计")
    sheet.addCell(label)
    sheet.addCell(label1)
    sheet.addCell(label2)

  }

   // second type sheet
  def sheetRowInit(sheet: XSSFSheet): Unit =  {

    val row = sheet.createRow(0)
    // filled first column
    row.createCell(0).setCellValue("股票")
    // filled first column
    row.createCell(1).setCellValue("小时")

    row.createCell(2).setCellValue("查看热度统计")

  }

  def  createXLSX(data: RDD[(String,Iterable[((String, String),Int)])]) = {

    val book = new XSSFWorkbook()

    data.sortBy(_._1).collect().foreach { x =>

      val path = x._1.split("-").slice(0, 2).mkString("-")  // 按月

      val os = new FileOutputStream(rootDir + "/" + path + ".xlsx", true)

      if(book.getSheet(x._1) != null) {

        val sheet = book.createSheet(x._1)

        sheetRowInit(sheet)

        var i = 1

        x._2.foreach { key =>
          // create first sheet and for each row filled with specific value
          val row = sheet.createRow(i)
          // filled first column
          row.createCell(0).setCellValue(key._1._1)
          // and second column
          row.createCell(1).setCellValue(key._1._2)
          row.createCell(2).setCellValue(key._2.toString)
          i += 1
        }
      }

      // write to file
      book.write(os)
      // close I/0
      os.close()

    }
  }

  def createXLS(data: RDD[(String,Iterable[((String, String),Int)])]) = {

    var book: WritableWorkbook = null

    var sheetNumber = 0

    data.sortBy(_._1).foreach { x =>

      val path = x._1.split("-").slice(0, 2).mkString("-")  // 按月
      val file = new File(rootDir + "/" + path + ".xls")
      // val book = Workbook.createWorkbook(file)

      if(!file.exists()) {
        file.createNewFile()
        book = Workbook.createWorkbook(file)
        sheetNumber = 0

      } else {

        book = Workbook.createWorkbook(file, Workbook.getWorkbook(file))
        val  numberOfSheet = book.getNumberOfSheets
        sheetNumber = numberOfSheet

      }

      val writerData = x._2
      println("writerData:" + writerData.size)

      var sheet = book.createSheet(x._1, sheetNumber)
      val label = new Label(0, 0, "股票") // 列，行
      val label1 = new Label(1, 0, "小时")
      val label2 = new Label(2, 0, "查看热度统计")
      sheet.addCell(label)
      sheet.addCell(label1)
      sheet.addCell(label2)

      var i = 1
      var k = 0

      writerData.foreach { key =>

        if(i > 60000) {
          sheetNumber += 1
          k = k + 1
          book.createSheet(x._1 + "-" + k, sheetNumber)
          sheet = book.getSheet(sheetNumber)
          // 初始化新sheet的模板
          sheetInit(sheet)
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
      book.write()
      book.close()
      sheetNumber += 1

    }
  }
}
