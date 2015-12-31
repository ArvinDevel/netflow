/**
  * Copyright 2015 ICT.
  *
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
/**
  * Created by arvin on 15-12-28.
  */
package cn.as.ict.acs.netflow.load.worker

import java.io._
import java.nio.ByteBuffer
import java.util

import cn.ac.ict.acs.netflow.load.worker.orc.OrcSchema
import cn.ac.ict.acs.netflow.load.worker.parquet.TimelyParquetWriter
import cn.ac.ict.acs.netflow.{NetFlowException, NetFlowConf}
import cn.ac.ict.acs.netflow.load.worker.Row
import cn.ac.ict.acs.netflow.load.worker.parser.PacketParser
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, CompressionKind}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{JavaIntObjectInspector, JavaBinaryObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable

object OrcTest extends FunSuite with BeforeAndAfter{

  import cn.ac.ict.acs.netflow.load.worker.orc.OrcWriter

  /* // trial no effect
  private val netflowConf = new NetFlowConf()
  private val orcWriter = new OrcWriter(0,System.currentTimeMillis(),netflowConf)

  private val fieldsName = collection.mutable.LinkedList("router_ipv4",
    "router_ipv6","l4_src_port")
  private val orcSchemaTypes = Array("binary","binary","int")

  // can't init ObjectInspector as below
//  private val fieldObejctInpsectors = collection.mutable.LinkedList(
  // new JavaBinaryObjectInspector(),
//    new JavaBinaryObjectInspector(), new JavaIntObjectInspector())

  val paras = s"struct<${ (0 until fieldsName.length).map(
    num => s"${fieldsName(num)}:${orcSchemaTypes(num)}").mkString(",")}>"
  val typeInfo =
    TypeInfoUtils.getTypeInfoFromTypeString(paras)

  private val structOI = TypeInfoUtils
    .getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
    .asInstanceOf[StructObjectInspector]
  before(
  println(paras)
  )

  def updateInspector(): Unit ={
    orcWriter.updateInspector(structOI)

  }


  */
  // to verify template keys ascend or not
  // v5 is out of order, but v9 maybe ordered
  // the data to be test maybe all v5
  // 12.28 Arvin
  // todo: find a way to terminate for loop in scala
  def testTemplateKeys(){
    val data = ByteBuffer.allocate(1550)

    val ip = Array(
      192.asInstanceOf[Byte],
      168.asInstanceOf[Byte],
      1.asInstanceOf[Byte],
      1.asInstanceOf[Byte])
    data.put(4.asInstanceOf[Byte])
    data.put(ip)

    val pos = data.position()


    val file = "/home/arvin/projects/NetFlow/netflow_receiver/testData/netflow5"
    val fi = new FileInputStream(file)
    val bf = new DataInputStream(fi)

    var i = 0
    while (bf.available() != 0) {
//      // bar access
//      if(i == 20) {
//        bf.close()
//      }
      val length = bf.readShort()
      data.position(pos)
      bf.read(data.array(), data.position(), length)
      val limit = data.position() + length

      data.flip()
      data.limit(limit)

      try {
        val (flowSets, packetTime) = PacketParser.parse(data)

        while(flowSets.hasNext)
          {
           val keysInTemplate = flowSets.next().template.keys

//            // test order
//            var tmp = -2
//            var flag = 1
//            for(key <- keysInTemplate){
//              if(key - tmp < 0) {
//                println("this template is not orderd")
//                flag = 0
//
//              }
//              tmp = key
//            }
//
//            // test content
//            if(flag > 0) {
//              println("begin Template")
//              keysInTemplate.map(println)
//              println("end ")
//            }
            // test if all v5
            if(keysInTemplate.head != 8){
              println("not v5")
            }
          }

        i += 1

      } catch {
        case e: NetFlowException => // println(e.getMessage)
      }
    }
  }


  // Calculation: var/val in function shouldn't be clarified starting with private
  // 12.28 Arvin
  import collection.mutable.{MutableList}
  def testOriginalOrc() {
    // data source one
    val mutableList = new mutable.MutableList[Any]()
    mutableList += "10.30.5.198"
    mutableList += 900
    // data source two
    val data = Array("192.168.1.1", 80)

    // data has null
    val testData = new Array[Any](2)
    testData(0) = "just a test"
    println(testData(1))

    // binary data in second position, and second type as binary
    val binary = Array[Byte](2,4)
    testData(1) = binary
    // bytebuffer is also ok?
    // exception: nio.heapbytebuffer can't be cast to [B
    // [B == byte[], binary use byte[]
    val byteBuffer = ByteBuffer.allocate(10)
    byteBuffer.put(binary)
//    testData(1) = byteBuffer


    val conf = new NetFlowConf()
    val fs = FileSystem.getLocal(conf.hadoopConfiguration)
    val fileName: String = "orcTest" + math.random
    val path = new Path("/tmp",fileName)
    val _conf = conf.hadoopConfiguration

    // construct inspector
    val fieldsName = Array("ip","port")
    val fieldsType = Array("string","binary")

    val paras = s"struct<${ (0 until fieldsName.length).map(
      num => s"${fieldsName(num)}:${fieldsType(num)}").mkString(",")}>"
    val typeInfo =
      TypeInfoUtils.getTypeInfoFromTypeString(paras)

    val inspector = TypeInfoUtils
      .getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
      .asInstanceOf[StructObjectInspector]

    // Todo: know the size, set properly
    // The writer stores the contents of the stripe in memory until this memory limit is reached
    // and the stripe is flushed to the HDFS file and the next stripe started.
    val stripeSize = 200000000L
    // NONE, LZO, SNAPPY, ZLIB
    val compress = CompressionKind.valueOf("ZLIB")
    // todo: goal like stripeSize
    val bufferSize = 10000
    // todo: set properly
    val rowIndexStride =  1000
    val writer = OrcFile.createWriter(fs,path,_conf,inspector,stripeSize,
      compress,bufferSize,rowIndexStride)

//    writer.addRow(mutableList.toArray)
    val ttestData = new Array[Any](2)
      writer.addRow(ttestData)
    writer.close()
  }

  def makeSmallTestdata(): Unit ={

    val inFilePath = "/home/arvin/projects/NetFlow/netflow_receiver/testData/netflow5"
    val outFilePath = "/tmp/netflow/netflow5"
    val fi = new FileInputStream(inFilePath)
    val bf = new DataInputStream(fi)

    val file = new File(outFilePath)
    file.createNewFile()
    val fo = new FileOutputStream(file)
    val bfo = new DataOutputStream(fo)

    val data = new Array[Byte](1550)
    for(i <- 0 until 100){
      val length = bf.readShort()
      bf.read(data, 0, length)
      bfo.writeShort(length)
      bfo.write(data,0,length)
    }

    bf.close()
    bfo.close()

  }
  def testOrcWriter(): Unit ={
    val data = ByteBuffer.allocate(1550)

    val ip = Array(
      192.asInstanceOf[Byte],
      168.asInstanceOf[Byte],
      1.asInstanceOf[Byte],
      1.asInstanceOf[Byte])
    data.put(4.asInstanceOf[Byte])
    data.put(ip)

    val pos = data.position()


    val file = "/home/arvin/projects/NetFlow/netflow_receiver/testData/netflow5"
    val fi = new FileInputStream(file)
    val bf = new DataInputStream(fi)

    val startTime = System.currentTimeMillis();
    val orcWriter = new OrcWriter(0,System.currentTimeMillis(),new NetFlowConf())
//    val parquetWriter = new TimelyParquetWriter(0,System.currentTimeMillis(),new NetFlowConf())
    var i = 0
    while (bf.available() != 0) {

      val length = bf.readShort()
      data.position(pos)
      bf.read(data.array(), data.position(), length)
      val limit = data.position() + length

      data.flip()
      data.limit(limit)

      try {
        val (flowSets, packetTime) = PacketParser.parse(data)

        while(flowSets.hasNext){
                    orcWriter.write(flowSets.next())
//          parquetWriter.write(flowSets.next())
        }
        i += 1


      } catch {
        case e: NetFlowException => // println(e.getMessage)
      }
    }
    bf.close()
    orcWriter.close()
//    parquetWriter.close()
    val end = System.currentTimeMillis()
    println("cost time is " + (end - startTime)/1000 + "s")
  }

  private def clearBuffer(row: Row,reusableOutputBuffer: Array[Any]) ={
    row.template.keys.foreach(k =>
      if (k != -1) {
        reusableOutputBuffer(k) = null
      })

  }
  def main(args: Array[String] ): Unit ={
//    testOriginalOrc()
//    testTemplateKeys
    testOrcWriter
//    makeSmallTestdata
  }


}

