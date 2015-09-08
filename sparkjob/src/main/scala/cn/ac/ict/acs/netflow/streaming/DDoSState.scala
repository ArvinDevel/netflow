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
package cn.ac.ict.acs.netflow.streaming

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable

import cn.ac.ict.acs.netflow.util.Utils

case class Statistic(dstIp: ByteBuffer, num: Int)

class DestinationInfo (
    @transient var dstIp: ByteBuffer,
    var multiDestination: Boolean,
    var count: Int) extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    out.writeInt(dstIp.limit())
    out.write(dstIp.array())
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    val data = new Array[Byte](in.readInt())
    in.readFully(data)
    dstIp = ByteBuffer.wrap(data)
  }

  def this(dstIp: ByteBuffer) {
    this(dstIp, false, 1)
  }

  def update(newDstIp: ByteBuffer): Unit = {
    if (newDstIp == dstIp) {
      count += 1
    } else {
      multiDestination = true
    }
  }

  def update(other: DestinationInfo): Unit = {
    if (dstIp == other.dstIp) {
      count += other.count
    } else {
      multiDestination = true
    }
  }
}

class SourceInfo private (
    var multiSource: Boolean,
    var count: Int) {

  def this(count: Int) {
    this(false, count)
  }

  def update(count: Int): Unit = {
    multiSource = true
    this.count += count
  }
}

class DDoSState extends Serializable {
  @transient var expireCounter: Int = 0
  // srcIp -> DstInfo
  @transient var srcMap: mutable.HashMap[ByteBuffer, DestinationInfo] = null
  // dstIp -> SrcInfo
  @transient var finalMap: mutable.HashMap[ByteBuffer, SourceInfo] = null

  def init(): Unit = {
    srcMap = new mutable.HashMap
  }

  init()

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeInt(expireCounter)
    out.writeInt(srcMap.size)
    srcMap.foreach { case (srcIp, dstInfo) =>
      out.writeInt(srcIp.limit)
      out.write(srcIp.array, 0, srcIp.limit())
      out.writeObject(dstInfo)
    }
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    init()
    expireCounter = in.readInt()
    val size = in.readInt()
    var i = 0
    while (i < size) {
      val srcIp = new Array[Byte](in.readInt())
      in.readFully(srcIp)
      val dstInfo = in.readObject().asInstanceOf[DestinationInfo]
      srcMap(ByteBuffer.wrap(srcIp)) = dstInfo
      i += 1
    }
  }

  def insert(srcIp: ByteBuffer, dstIp: ByteBuffer): Unit = {
    srcMap.get(srcIp) match {
      case Some(v) => v.update(dstIp)
      case None => val d = new DestinationInfo(dstIp); srcMap(srcIp) = d
    }
  }

  def merge(other: DDoSState): DDoSState = {
    other.srcMap.foreach {
      case (srcIp, dstInfo) =>
        srcMap.get(srcIp) match {
          case Some(v) => v.update(dstInfo)
          case None => srcMap(srcIp) = dstInfo
        }
    }
    this
  }

  def build(): Double = {
    // filter out srcIp with multiple destination
    val singleDstSrc = srcMap.filterNot(_._2.multiDestination)

    val dstWithMultipleSrc = mutable.HashMap.empty[ByteBuffer, SourceInfo]

    // filter out dstIp with only one srcIp
    singleDstSrc.foreach {
      case (srcIp, dstInfo) =>
        dstWithMultipleSrc.get(dstInfo.dstIp) match {
          case Some(v) => v.update(dstInfo.count)
          case None => val s = new SourceInfo(dstInfo.count); dstWithMultipleSrc(dstInfo.dstIp) = s
        }
    }
    finalMap = dstWithMultipleSrc.filter(_._2.multiSource)
    var total = 0.0
    finalMap.foreach(total += _._2.count)
    val m = finalMap.size
    (total - m) / m
  }

  def output(threshold: Double): Seq[Statistic] = {
    finalMap.filter(_._2.count > threshold).map {
      case (dstIP, sourceInfo) =>
        Statistic(dstIP, sourceInfo.count)
    }.toSeq
  }
}
