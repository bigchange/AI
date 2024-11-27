package com.bigchange.kafka

import com.bigchange.util.TimeUtil
import org.apache.spark.sql.{ForeachWriter, Row}

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}

class MyLocalBatchWriter(tempDir: String, finalDir: String)
    extends ForeachWriter[Row] {
  var fileWriter: BufferedWriter = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    val tempFilePath = s"$tempDir/partition-$partitionId-epoch-$epochId.txt"
    fileWriter = new BufferedWriter(new FileWriter(tempFilePath))
    true
  }

  override def process(row: Row): Unit = {
    fileWriter.write(row.getString(0))
    fileWriter.newLine()
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (fileWriter != null) {
      fileWriter.flush()
      fileWriter.close()
    }
  }
}
object MyLocalBatchWriter {

  private def listAllFiles(
      dir: File,
      fileFilter: (String => Boolean)
  ): Array[File] = {
    val these = dir.listFiles
    these.filter(item => item.isFile && fileFilter(item.getName)) ++ these
      .filter(_.isDirectory)
      .flatMap(listAllFiles(_, fileFilter))
  }

  def mergeFiles(tempDir: String, finalDir: String): Unit = {
    // 定期合并逻辑
    val tempFiles = listAllFiles(
      new File(tempDir),
      fileFilter = { item =>
        item != "_SUCCESS"
      }
    )
    if (tempFiles.nonEmpty) {
      // 打开目标文件的 FileChannel
      val currentDayWithHour = TimeUtil.getCurrentDayWithHour
      val day = TimeUtil.getCurrentDay
      val finalPath =
        Paths.get(finalDir, day, s"access_log_${currentDayWithHour}").toString
      val targetPath = Paths.get(finalPath)
      // 自动创建目标目录
      if (!Files.exists(targetPath.getParent)) {
        Files.createDirectories(targetPath.getParent)
      }
      // 确保目标文件路径存在
      if (!Files.exists(targetPath)) {
        Files.createFile(targetPath) // 如果文件不存在则创建
      }
      val targetChannel =
        FileChannel.open(targetPath, StandardOpenOption.APPEND)
      try {
        for (sourceFile <- tempFiles) {
          val sourcePath = Paths.get(sourceFile.getPath)
          val sourceChannel =
            FileChannel.open(sourcePath, StandardOpenOption.READ)
          // 复制文件内容到目标文件
          sourceChannel.transferTo(0, sourceChannel.size(), targetChannel)
          sourceChannel.close()
          sourceFile.delete()
        }
      } finally {
        targetChannel.close()
      }
    }
  }
}
