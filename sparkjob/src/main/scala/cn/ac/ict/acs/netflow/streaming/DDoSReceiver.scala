package cn.ac.ict.acs.netflow.streaming

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import cn.ac.ict.acs.netflow.{NetFlowException, Logging}

case class Record(time: Long, srcIp: Array[Byte], dstIp: Array[Byte])

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
    channel.configureBlocking(false)
    channel.connect(lwAddress)

    val packet = new Packet
    while(!channel.finishConnect()) {}

    try {
      while (true) {
        channelRead(channel, packet.countAndTime)
        while (packet.countAndTime.position < 12) {
          channelRead(channel, packet.countAndTime)
        }
        val count = packet.countAndTime.getInt()
        val time = packet.countAndTime.getLong()
        packet.content = ByteBuffer.allocate(count * 8)
        val currentContent = packet.content

        while (currentContent.position != currentContent.capacity) {
          channelRead(channel, currentContent)
        }

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
