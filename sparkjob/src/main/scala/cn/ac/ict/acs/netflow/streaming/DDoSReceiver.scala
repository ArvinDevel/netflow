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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.text.SimpleDateFormat
import java.util.Date

import cn.ac.ict.acs.netflow.util.IPUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import cn.ac.ict.acs.netflow.{NetFlowException, Logging}

case class Record(time: Long, srcIp: Array[Byte], dstIp: Array[Byte]) {

  val format = new SimpleDateFormat("HH:mm:ss.SSS")

  def timeString = format.format(new Date(time))
  def srcIPString = IPUtils.toString(srcIp)
  def dstIPString = IPUtils.toString(dstIp)
}

class DDoSReceiver(val host: String, val port: Int)
  extends Receiver[Record](StorageLevel.MEMORY_AND_DISK_2)
  with Logging {

  override def preferredLocation = Some(host)

  override def onStart() = {
    // Start the thread that receives data over a connection
      new Thread("Socket Receiver") {
        override def run() { receive() }
      }.start()

  }

  override def onStop() = {
    // do nothing for the moment
  }

  private[this] def receive(): Unit = {
    def channelRead(channel: SocketChannel, buffer: ByteBuffer): Unit = {
      if (channel.read(buffer) < 0) throw new NetFlowException("Channel closed")
    }

    val lwAddress = new InetSocketAddress(host, port)
    val channel = SocketChannel.open()
//    channel.configureBlocking(false)
    channel.connect(lwAddress)

    val packet = new Packet
    while(!channel.finishConnect()) {}

    try {
      while (true) {
        channelRead(channel, packet.countAndTime)
        while (packet.countAndTime.position < 12) {
          channelRead(channel, packet.countAndTime)
        }
        packet.countAndTime.flip()
        val count = packet.countAndTime.getInt()
        val time = packet.countAndTime.getLong()
        packet.content = ByteBuffer.allocate(count * 8)
        val currentContent = packet.content

        while (currentContent.position != currentContent.capacity) {
          channelRead(channel, currentContent)
        }
        currentContent.flip()

        var i = 0
        while (i < count) {
          val srcIp = new Array[Byte](4)
          val dstIp = new Array[Byte](4)
          store(Record(time,
            {currentContent.get(srcIp); srcIp},
            {currentContent.get(dstIp); dstIp}))
          i += 1
        }
        packet.countAndTime.clear()
        packet.content = null
      }
    } catch {
      case e: NetFlowException =>
        logWarning(s"Error occurs while reading packets: ${e.getMessage}")
        channel.close()
    }
  }
}

class Packet {
  val countAndTime: ByteBuffer = ByteBuffer.allocate(12)
  var content: ByteBuffer = _
}
