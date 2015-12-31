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
package cn.ac.ict.acs.netflow.load.worker

import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.load.worker.parser.Template

// time: Long, routerIpv4: Array[Byte], routerIpv6: Array[Byte]
class RowHeader(val fields: Array[Any])

abstract class Row {
  def header: RowHeader
  def bb: ByteBuffer
  def startPos: Int
  def template: Template
  final def length: Int = {
    require(template != null)
    template.rowLength
  }
<<<<<<< HEAD
  // not reset position, called by OrcWriter to write bgp
  // 12.30 Arvin
  lazy val ipv4OrV6DstAddr: Array[Byte] = {
    val v4dstIndex = template.keys.indexOf(12)
    val v6dstIndex = template.keys.indexOf(28)
    if (v4dstIndex != -1) {
      val ipPosInCurRow = template.values.take(v4dstIndex).sum
      val ipv4DstAddr = new Array[Byte](4)
      bb.position(startPos + ipPosInCurRow)
      bb.get(ipv4DstAddr, 0, 4)
      ipv4DstAddr
    }else if(v6dstIndex != -1){
      val ipPosInCurRow = template.values.take(v6dstIndex).sum
      val ipv6DstAddr = new Array[Byte](16)
      bb.position(startPos + ipPosInCurRow)
      bb.get(ipv6DstAddr, 0, 16)
      ipv6DstAddr
    }else{
     new Array[Byte](0)
    }
  }
=======
>>>>>>> 0ff90c74ba797a32bdea0460b0d8f9a1c5175746
}

class MutableRow(val bb: ByteBuffer, val template: Template) extends Row {
  var header: RowHeader = _
  var startPos: Int = _

  def setHeader(rowheader: RowHeader): Unit = {
    header = rowheader
  }

  /**
   * a new row appears
   * @param start
   */
  def update(start: Int): Unit = {
    this.startPos = start
  }
}

