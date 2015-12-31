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
package cn.ac.ict.acs.netflow.load.worker.tcp

import java.net.{ServerSocket, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels._

import scala.collection.mutable

import cn.ac.ict.acs.netflow.{NetFlowException, NetFlowConf, Logging}
import cn.ac.ict.acs.netflow.load.worker.WrapBufferQueue
import cn.ac.ict.acs.netflow.load.worker.parser.Template
import cn.ac.ict.acs.netflow.load.worker.parser.PacketParser._
import cn.ac.ict.acs.netflow.util.Utils
import cn.ac.ict.acs.netflow.load.util.ByteBufferPool

class StreamingServer(
    queue: WrapBufferQueue,
    pool: ByteBufferPool,
    conf: NetFlowConf) extends Thread with Logging {
  logInfo(s"Initializing Server for SparkStreaming Receiver")
  setName("SparkStreaming Server Thread")
  setDaemon(true)

  private var selector: Selector = _

  private val channels = mutable.HashSet.empty[Channel]
  private val channelToIp = mutable.HashMap.empty[Channel, String]

  var _port = 0
  def port: Int = {
    while (_port == 0) Thread.sleep(500)
    _port
  }
  def streamingReceivers: Iterable[String] = channelToIp.values

  override def run() = {
    val serverSocketChannel = ServerSocketChannel.open()
    serverSocketChannel.configureBlocking(false)
    val serverSocket = serverSocketChannel.socket()
    channels += serverSocketChannel

    val (_, actualPort) =
      Utils.startServiceOnPort(0, startListening(serverSocket), conf, "SparkStreaming server")
    logInfo(s"Server is listening $actualPort for Streaming Receiver connections")
    _port = actualPort

    selector = Selector.open()
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)

    try {
      while (true) {
        try {
          // Blocking until channel of interest appears
          selector.select()

          val iter = selector.selectedKeys().iterator()
          while (iter.hasNext) {
            val key = iter.next()
            if (key.isAcceptable) {
              registerChannel(key)
            } else if (key.isWritable) {
              writeDataToSocket(key)
            }
            iter.remove()
          }
        } catch {
          case e: Throwable =>
            logWarning(s"Exception occurs during runtime: ${e.getStackTraceString}")
          // TODO
        }
      }
    } finally {
      // When a selector is closed, all channels registered with that selector are deregistered,
      // and the associated keys are invalidated(cancelled)
      logWarning("SparkStreaming Server thread terminated")
      selector.close()
      channels.foreach(_.close())
    }
  }

  private def startListening(serverSocket: ServerSocket): (Int) => (Any, Int) = { tryPort =>
    if (tryPort == 0) {
      serverSocket.bind(null)
    } else {
      serverSocket.bind(new InetSocketAddress(tryPort))
    }
    (null, serverSocket.getLocalPort)
  }

  /**
   * Accept remote connection
   */
  private def registerChannel(sk: SelectionKey): Unit = {
    // connect socket
    val socketChannel = sk.channel().asInstanceOf[ServerSocketChannel].accept()
    socketChannel.configureBlocking(false)
    socketChannel.register(selector, SelectionKey.OP_WRITE)

    val remoteIP = socketChannel.getRemoteAddress.asInstanceOf[InetSocketAddress]
      .getAddress.getHostAddress
    channels += socketChannel
    channelToIp(socketChannel) = remoteIP
    logInfo(s"Open connection from $remoteIP.")
  }

  private def writeDataToSocket(sk: SelectionKey): Unit = {

    val channel = sk.channel().asInstanceOf[SocketChannel]
    try {
      val buffer = toSend()
      while (buffer.hasRemaining) {
        channel.write(buffer)
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Error occurs while writing packets: ${e.getMessage}")
        close(sk)
    }
  }

  private def close(key: SelectionKey): Unit = {
    // When a channel is closed, all keys associated with it are automatically cancelled
    key.attach(null)
    val c = key.channel()
    channels -= c
    channelToIp.remove(c)
    c.close()
  }

  val resultBuffer = ByteBuffer.allocate(2048)

  def toSend(): ByteBuffer = {
    resultBuffer.clear()
    resultBuffer.position(4) // leave for line count

    var currentPacket = queue.currentPacket
    var invalidPacket = true

    while (invalidPacket) {
      currentPacket.synchronized {
        if (currentPacket.holderNum.get() == 1) {
          invalidPacket = false
          currentPacket.holderNum.incrementAndGet()
        } else {
          currentPacket = queue.currentPacket
        }
      }
    }

    val currentContent = currentPacket.buffer.duplicate()

    val (flowSets, packetTime) = parse(currentContent)
    resultBuffer.putLong(packetTime)
    var count = 0
    while (flowSets.hasNext) {
      val rows = flowSets.next().getRows

      var first = true
      var srcPos = -1
      var dstPos = -1

      try {
        while (rows.hasNext) {
          val row = rows.next()

          if (first) {
            // all rows in the same flowSet share a common template,
            // just handle once here
            val template = row.template
            val tp = getIpPos(template)
            srcPos = tp._1;
            dstPos = tp._2
            first = false
          }

          if (srcPos == -1 || dstPos == -1) {
            // not a valid flowSet, just ignore it
            throw new NetFlowException("Not a valid flowset, just ignore this")
          }

          currentContent.position(row.startPos + srcPos)
          transferIpv4(currentContent, resultBuffer)
          currentContent.position(row.startPos + dstPos)
          transferIpv4(currentContent, resultBuffer)

          count += 1
        }
      } catch {
        case e: NetFlowException => // do nothing
      }
    }

    pool.release(currentPacket)

    resultBuffer.putInt(0, count)
    resultBuffer.flip().asInstanceOf[ByteBuffer]
  }

  // currently we only handle ipv4 src and dst
  private[this] def getIpPos(template: Template): (Int, Int) = {
    var ipSrcPos = 0
    var ipDstPos = 0
    var i = 0
    var startPos = 0
    while (i < template.keys.length) {
      if (template.keys(i) == 8) {
        ipSrcPos = startPos
      } else if (template.keys(i) == 12) {
        ipDstPos = startPos
      }
      startPos += template.values(i)
      i += 1
    }
    (ipSrcPos, ipDstPos)
  }

  private[this] def transferIpv4(from: ByteBuffer, to: ByteBuffer): Unit = {
    to.put(from.get); to.put(from.get); to.put(from.get); to.put(from.get)
  }
}
