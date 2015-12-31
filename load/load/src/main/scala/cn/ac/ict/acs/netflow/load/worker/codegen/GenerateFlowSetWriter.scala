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

package cn.ac.ict.acs.netflow.load.worker.codegen

import org.apache.parquet.schema.OriginalType.UTF8
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.io.api.RecordConsumer

import cn.ac.ict.acs.netflow.load.util.BytesUtil
import cn.ac.ict.acs.netflow.load.worker.bgp.BGPRoutingTable
import cn.ac.ict.acs.netflow.load.worker.DataFlowSet
import cn.ac.ict.acs.netflow.load.worker.parquet.ParquetSchema
import cn.ac.ict.acs.netflow.load.worker.parser.Template

abstract class InnerWriter {
  def insert(consumer: RecordConsumer, dataFlowSet: DataFlowSet): Unit
}

object GenerateFlowSetWriter extends CodeGenerator[Template, InnerWriter] {
  import ParquetSchema._


  protected def create(in: Template): InnerWriter = {
    val ctx = newCodeGenContext()
    val code = s"""
    public SpecificWriter generate($templateType template) {
      return new SpecificWriter();
    }

    // Writer of template: ${in.tmpId}
    class SpecificWriter extends ${classOf[InnerWriter].getName} {
      @Override
      public void insert(RecordConsumer consumer, DataFlowSet dataFlowSet) {
        int curRowPos = dataFlowSet.startPos() + dataFlowSet.fsHeaderLen();

        int endPos = dataFlowSet.endPos();
        ByteBuffer bb = dataFlowSet.bb();
        long packetTime = dataFlowSet.packetTime();
        byte[] routerIp = dataFlowSet.routerIp();

        while (curRowPos != endPos) {
          bb.position(curRowPos);
          consumer.startMessage();

          // write header
          $headerWrite

          // write netflow payload
          ${netflowWrite(in, ctx)}

          // write bgp info
          ${bgpWrite(in)}

          consumer.endMessage();
          curRowPos += ${in.rowLength};
        }
      }
    }
    """
//    println(code)
    compile(code).generate(in).asInstanceOf[InnerWriter]
  }

  private def headerWrite(): String = {
    s"""
      consumer.startField("time", 0);
      consumer.addLong(packetTime);
      consumer.endField("time", 0);
      if (routerIp.length == 4) {
        consumer.startField("router_ipv4", 1);
        consumer.addBinary(Binary.fromByteArray(routerIp));
        consumer.endField("router_ipv4", 1);
      } else {
        consumer.startField("router_ipv6", 2);
        consumer.addBinary(Binary.fromByteArray(routerIp));
        consumer.endField("router_ipv6", 2);
      }
     """
  }

  private def netflowWrite(in: Template, ctx: CodeGenContext): String = {
    val bytesUtil = BytesUtil.getClass.getName.stripSuffix("$")
    in.keys.zip(in.values).map { case (k, v) =>
      if (k != -1) {
        val (pos, tpe) = getPosAndType(FieldType.NETFLOW, k)
        val writeField = tpe.asPrimitiveType.getPrimitiveTypeName match {
          case INT64 => // network byte order is big-endian
            s"consumer.addLong($bytesUtil.fieldAsBELong(bb, $v));"
          case INT32 => // network byte order is big-endian
            s"consumer.addInteger($bytesUtil.fieldAsBEInt(bb, $v));"
          case BINARY | FIXED_LEN_BYTE_ARRAY =>
            val bytes = ctx.freshName("bytes")
            s"""byte[] $bytes = new byte[$v];
            bb.get($bytes, 0, $v);
            consumer.addBinary(Binary.fromByteArray($bytes));"""
        }
        s"""
          consumer.startField("${tpe.getName()}", $pos);
          $writeField
          consumer.endField("${tpe.getName()}", $pos);
         """
      } else {
        s"          bb.position(bb.position() + $v);"
      }
    }.mkString("\n")
  }


  // 12 28 is the index in netflowFields, cauze Template merge header and netflowFields,
  // the index should be 14 and 30
  // 12.29 Arvin
  // todo : verify sentence above

  private def bgpWrite(in: Template): String = {
    val bgpTable = BGPRoutingTable.getClass.getName.stripSuffix("$")
    val v4dstIndex = in.keys.indexOf(12)
    val v6dstIndex = in.keys.indexOf(28)
    val bgpSearch = if (v4dstIndex != -1) {
      val ipPosInCurRow = in.values.take(v4dstIndex).reduce(_ + _)
      s"""
        byte[] ipv4DstAddr = new byte[4];
        bb.position(curRowPos + $ipPosInCurRow);
        bb.get(ipv4DstAddr, 0, 4);
        bb.position(curRowPos);
        Object[] bgpTuple = $bgpTable.search(ipv4DstAddr);
      """
    } else if (v6dstIndex != -1) {
      val ipPosInCurRow = in.values.take(v6dstIndex).reduce(_ + _)
      s"""
        byte[] ipv6DstAddr = new byte[16];
        bb.position(curRowPos + $ipPosInCurRow);
        bb.get(ipv6DstAddr, 0, 16);
        bb.position(curRowPos);
        Object[] bgpTuple = $bgpTable.search(ipv6DstAddr); // we may want to specialize v6 from v4
      """
    } else {
      s"Object[] bgpTuple = $bgpTable.search(new byte[0]);"
    }

    bgpSearch + (0 until validBgp.length).map { i =>
      val (pos, tpe) = getPosAndType(FieldType.BGP, i)
      tpe.asPrimitiveType().getPrimitiveTypeName match {
        case BINARY if tpe.getOriginalType == UTF8 =>
          s"""
           if (bgpTuple[$i] != null) {
             consumer.startField("${tpe.getName()}", $pos);
             consumer.addBinary(Binary.fromString((String) bgpTuple[$i]));
             consumer.endField("${tpe.getName()}", $pos);
           }
          """
        case BINARY =>
          s"""
           if (bgpTuple[$i] != null) {
             consumer.startField("${tpe.getName()}", $pos);
             consumer.addBinary(Binary.fromByteArray((byte[]) bgpTuple[$i]));
             consumer.endField("${tpe.getName()}", $pos);
           }
          """
      }
    }.mkString("\n")
  }
}
