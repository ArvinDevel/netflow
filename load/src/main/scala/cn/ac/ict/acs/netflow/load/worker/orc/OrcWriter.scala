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
  * Created by arvin on 15-12-26.
  *
  *
  *
  */
package cn.ac.ict.acs.netflow.load.worker.orc
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.load.worker.bgp.BGPRoutingTable
import cn.ac.ict.acs.netflow.load.worker.{DataFlowSet, Row, Writer}
import cn.ac.ict.acs.netflow.util.Utils
import cn.ac.ict.acs.netflow.{Logging, NetFlowConf, load}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{CompressionKind, OrcFile, OrcSerde}
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils

// OrcWriter, the contract is below:
// 1)first call init() to init OrcWriter
// 2)

class OrcWriter (val id: Int, val timeBase: Long, val conf: NetFlowConf
                )
  extends Writer with Logging {

  import OrcWriter._
  // NetFlow Schema
  private val fieldNames = OrcSchema.overallSchema
  // schema discribed by Orc
  private val orcSchemaTypes = OrcSchema.overallSchemaTypes


  // use hadoopConf, don't know it's true or not !!!
  // RecordWriter use serializer, orcwriter not
  private val serializer = {
    val table = new Properties()
    table.setProperty("columns", fieldNames.mkString(","))
    table.setProperty("columns.types", orcSchemaTypes.mkString(":"))

    val serde = new OrcSerde
    val configuration = conf.hadoopConfiguration
    serde.initialize(configuration, table)
    serde
  }

  // Object inspector converted from the schema of the relation to be written.
  private val structOI = {
    // scala suggest me to replace range by indices.
    // Accept 12.28 Arvin
    val typeInfo =
      TypeInfoUtils.getTypeInfoFromTypeString(
        s"struct<${ fieldNames.indices.map(
          num => s"${fieldNames(num)}:${orcSchemaTypes(num)}").mkString(",")}>")

    TypeInfoUtils
      .getStandardJavaObjectInspectorFromTypeInfo(typeInfo)
      .asInstanceOf[StructObjectInspector]
  }

  // copyed from ParquetWriter
  private val outputFile: Path = {
    val basePath = if (timeBase > 0) {
      load.getPathByTime(timeBase, conf)
    } else {
      // we are a writer collecting unsorted packets
      load.systemBasePath + "/" + conf.get(LoadConf.UNSORTED_PACKETS_DIR, "unsorted")
    }
    val fileName = "%s-%02d-%d.orc".
      format(Utils.localHostName(), fileId.getAndIncrement(), System.currentTimeMillis())
    new Path(new Path(basePath, LoadConf.TEMP_DIRECTORY), fileName)

  }

  // just use Orc Writer to write
  // configuration for orc writer
  import org.apache.hadoop.fs._
  private val fs = FileSystem.getLocal(conf.hadoopConfiguration)

  // Note: val clarification has order, if put outputFile after path, IDE
  // has forward reference warning. when writer.close() is called,
  // there is NUllPointer!!!
  //
  private val path = outputFile
  //  private val execu = {
  //    println("test outFile value(if before, has value) : " + outputFile.toString)
  //  }
  private val _conf = conf.hadoopConfiguration
  private var inspector: ObjectInspector = structOI
  // Todo: know the size, set properly
  // The writer stores the contents of the stripe in memory until this memory limit is reached
  // and the stripe is flushed to the HDFS file and the next stripe started.
  val stripeSize =
    conf.getLong(LoadConf.ORC_STRIPE_SIZE,40000000L)
  // NONE, LZO, SNAPPY, ZLIB
  private val compress = CompressionKind.valueOf(conf.get(LoadConf.ORC_COMPRESSION,"NONE"))
  // todo: goal like stripeSize
  private val bufferSize = conf.getInt(LoadConf.ORC_BUFFER_SIZE,10000)
  // todo: set properly
  private val rowIndexStride = conf.getInt(LoadConf.ORC_ROWINDEX_STRIDE,1000)

  private var writer = OrcFile.createWriter(fs,outputFile,_conf,inspector,stripeSize,
    compress,bufferSize,rowIndexStride)


  // Used to hold temporary `Writable` fields of the next row to be written.
  private val reusableOutputBuffer = new Array[Any](fieldNames.length)

  // previously want to use func to update inspector,
  // currently use overall schema, so there is no need.
  // 12.28 Arvin
  def updateInspector(newInspector: ObjectInspector): Unit ={
    inspector = newInspector
    writer = OrcFile.createWriter(fs,path,_conf,inspector,stripeSize,
      compress,bufferSize,rowIndexStride)

  }


  // write the flowSet use array Type
  override def write(flowSet: DataFlowSet): Unit ={

    // write header part
    reusableOutputBuffer(0) = ByteBuffer.allocate(8).putLong(flowSet.packetTime).array()
    if(flowSet.routerIp.length == 4)
    {
      reusableOutputBuffer(1) = flowSet.routerIp
    }
    else
    {
      reusableOutputBuffer(2) = flowSet.routerIp
    }

    val iter = flowSet.getRows
    while(iter.hasNext){
      val row = iter.next()
      val template = row.template
      val byteBuffer: ByteBuffer = row.bb
      var tmpValue = new Array[Byte](4)
      template.keys.zip(template.values).map {
        case (k, v) =>
          if (k != -1) {

            // use too much byte[]
            // todo: optimize  it
            tmpValue = new Array[Byte](v)
            byteBuffer.get(tmpValue, 0, v)
            reusableOutputBuffer(k) = tmpValue
          }
          else {
            byteBuffer.position(byteBuffer.position() + v)
          }
      }
      // add bgp info
            val bgpTuple = BGPRoutingTable.search(row.ipv4OrV6DstAddr)
            OrcSchema.validBgp.indices.foreach(i =>
              reusableOutputBuffer(OrcSchema.bgpStartPos + i) = bgpTuple(i))

      // Write to File through orcWriter
      writer.addRow(reusableOutputBuffer)

      // to verify content is not the last,on the other hand,
      // or that will be nullPointer
      clearBuffer(row.template.keys)
      // clear bpg info
      OrcSchema.validBgp.indices.foreach(i =>
        reusableOutputBuffer(OrcSchema.bgpStartPos + i) = null)
    }
    // clear header
    reusableOutputBuffer(0) = null
    reusableOutputBuffer(1) = null
    reusableOutputBuffer(2) = null

  }


  // should care keys are not ordered! not a problem
  // byte[] type in orc? binary Accept
  // get string from byte[] unAdopt
  // 12.29 Arvin
  // Note: cause NullPointerError!!
  // two vital sources: function call and uninitialization
  // even variable in map will be clean(it's a func)
  @deprecated("Not used")
  private def row2Buffer(row: Row): Unit ={



  }

  private def clearBuffer(keys: Array[Int]) ={
    keys.foreach(k =>
      if (k != -1) {
        reusableOutputBuffer(k) = null
      })

  }
  /*
  // Used to hold temporary `Writable` fields of the next row to be written.
  private val reusableOutputBuffer = new Array[Any](fieldNames.length)


  // `OrcRecordWriter.close()` creates an empty file if no rows are written at all.  We use this
  // flag to decide whether `OrcRecordWriter.close()` needs to be called.
  private var recordWriterInstantiated = false

  private lazy val recordWriter: RecordWriter[NullWritable, Writable] = {
    recordWriterInstantiated = true

    val conf = getConfigurationFromJobContext(context)
    val uniqueWriteJobId = conf.get("spark.sql.sources.writeJobUUID")

    new OrcOutputFormat().getRecordWriter(
      outputFile.getFileSystem(conf),
      conf.asInstanceOf[JobConf],
      outputFile.toString,
      Reporter.NULL
    ).asInstanceOf[RecordWriter[NullWritable, Writable]]
  }

  override def write(flowSet: DataFlowSet): Unit ={

    prepareForWrite(flowSet)

    recordWriter.write(
      NullWritable.get(),
      serializer.serialize(reusableOutputBuffer, structOI))


  }
  override def close(): Unit = {
    if (recordWriterInstantiated) {
      recordWriter.close(Reporter.NULL)
    }
  }

  */

  override def init(): Unit = {

  }

  override def close(): Unit = {
    writer.close()
  }
}

object OrcWriter{
  private val writerId = new AtomicInteger(0)

  private val fileId = new AtomicInteger(0)
}



