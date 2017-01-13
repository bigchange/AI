package com.bigchange.hbase

import java.util

import com.google.protobuf.Descriptors.MethodDescriptor
import com.google.protobuf.{Message, Service}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.{ResultScanner, Row, _}
import org.apache.hadoop.hbase.client.coprocessor.Batch.{Call, Callback}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel

/**
  * Created by C.J.YOU on 2017/1/13.
  */
class HTable(tableName: String, columnFamilies:List[String])  extends  Table {

  def getName: String = tableName

  def getColumnFamilies: List[String] =  columnFamilies

  override def checkAndMutate(bytes: Array[Byte], bytes1: Array[Byte], bytes2: Array[Byte], compareOp: CompareOp, bytes3: Array[Byte], rowMutations: RowMutations): Boolean = ???

  override def incrementColumnValue(bytes: Array[Byte], bytes1: Array[Byte], bytes2: Array[Byte], l: Long): Long = ???

  override def incrementColumnValue(bytes: Array[Byte], bytes1: Array[Byte], bytes2: Array[Byte], l: Long, durability: Durability): Long = ???

  override def batch(list: util.List[_ <: Row], objects: Array[AnyRef]): Unit = ???

  override def batch(list: util.List[_ <: Row]): Array[AnyRef] = ???

  override def get(get: Get): Result = ???

  override def get(list: util.List[Get]): Array[Result] = ???

  override def checkAndPut(bytes: Array[Byte], bytes1: Array[Byte], bytes2: Array[Byte], bytes3: Array[Byte], put: Put): Boolean = ???

  override def checkAndPut(bytes: Array[Byte], bytes1: Array[Byte], bytes2: Array[Byte], compareOp: CompareOp, bytes3: Array[Byte], put: Put): Boolean = ???

  override def getTableDescriptor: HTableDescriptor = ???

  override def getWriteBufferSize: Long = ???

  override def put(put: Put): Unit = ???

  override def put(list: util.List[Put]): Unit = ???

  override def mutateRow(rowMutations: RowMutations): Unit = ???

  override def increment(increment: Increment): Result = ???

  override def append(append: Append): Result = ???

  override def delete(delete: Delete): Unit = ???

  override def delete(list: util.List[Delete]): Unit = ???

  override def existsAll(list: util.List[Get]): Array[Boolean] = ???

  override def coprocessorService(bytes: Array[Byte]): CoprocessorRpcChannel = ???

  override def coprocessorService[T <: Service, R](aClass: Class[T], bytes: Array[Byte], bytes1: Array[Byte], call: Call[T, R]): util.Map[Array[Byte], R] = ???

  override def coprocessorService[T <: Service, R](aClass: Class[T], bytes: Array[Byte], bytes1: Array[Byte], call: Call[T, R], callback: Callback[R]): Unit = ???

  override def batchCallback[R](list: util.List[_ <: Row], objects: Array[AnyRef], callback: Callback[R]): Unit = ???

  override def batchCallback[R](list: util.List[_ <: Row], callback: Callback[R]): Array[AnyRef] = ???

  override def close(): Unit = ???

  override def batchCoprocessorService[R <: Message](methodDescriptor: MethodDescriptor, message: Message, bytes: Array[Byte], bytes1: Array[Byte], r: R): util.Map[Array[Byte], R] = ???

  override def batchCoprocessorService[R <: Message](methodDescriptor: MethodDescriptor, message: Message, bytes: Array[Byte], bytes1: Array[Byte], r: R, callback: Callback[R]): Unit = ???

  override def exists(get: Get): Boolean = ???

  override def setWriteBufferSize(l: Long): Unit = ???

  override def getConfiguration: Configuration = ???

  override def checkAndDelete(bytes: Array[Byte], bytes1: Array[Byte], bytes2: Array[Byte], bytes3: Array[Byte], delete: Delete): Boolean = ???

  override def checkAndDelete(bytes: Array[Byte], bytes1: Array[Byte], bytes2: Array[Byte], compareOp: CompareOp, bytes3: Array[Byte], delete: Delete): Boolean = ???

  override def getScanner(scan: Scan): ResultScanner = ???

  override def getScanner(bytes: Array[Byte]): ResultScanner = ???

  override def getScanner(bytes: Array[Byte], bytes1: Array[Byte]): ResultScanner = ???
}
