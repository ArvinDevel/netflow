package cn.ac.ict.acs.netflow.load.worker.tcp

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import scala.util.control.Breaks._

import akka.actor.{ Actor, ActorRef, Props }
import akka.io.{ IO, Tcp }
import akka.util.ByteString

import cn.ac.ict.acs.netflow.Logging
import cn.ac.ict.acs.netflow.load.LoadMessages.StreamingPort
import cn.ac.ict.acs.netflow.load.worker.WrapBufferQueue
import cn.ac.ict.acs.netflow.load.worker.parser.Template
import cn.ac.ict.acs.netflow.load.worker.parser.PacketParser._
import cn.ac.ict.acs.netflow.util.{Utils, ActorLogReceive}

class StreamingServer(queue: WrapBufferQueue, worker: ActorRef)
  extends Actor with ActorLogReceive with Logging {
  import Tcp._
  import context.system

  override def preStart() = {
    IO(Tcp) ! Bind(self, new InetSocketAddress(Utils.localHostName(), 0))
  }

  def receiveWithLogging = {
    case Bound(localAddress) =>
      val port = localAddress.getPort
      logInfo(s"Bound at $port for StreamingReceiver connections.")
      worker ! StreamingPort(port)

    case CommandFailed(_: Bind) =>
      context.stop(self)

    case Connected(remote, local) =>
      logInfo(s"$remote connected.")
      val handler = context.actorOf(Props(classOf[RowSender]))
      val connection = sender()
      connection ! Register(handler)
  }
}

class RowSender(connection: ActorRef, remote: InetSocketAddress, queue: WrapBufferQueue)
  extends Actor with ActorLogReceive with Logging {
  import Tcp._
  context.watch(connection)

  case object Ack extends Event

  val resultBuffer = ByteBuffer.allocate(2048)

  def toSend(): ByteBuffer = {
    resultBuffer.clear()
    resultBuffer.position(4) // leave for line count

    // duplicate first so that we didn't change the origin one
    val currentPacket = queue.currentPacket.duplicate()

    val (flowSets, packetTime) = parse(currentPacket)
    resultBuffer.putLong(packetTime)
    var count = 0
    while (flowSets.hasNext) {
      val rows = flowSets.next().getRows

      var first = true
      var srcPos = -1
      var dstPos = -1

      breakable {
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
            break()
          }
          
          currentPacket.position(row.startPos + srcPos)
          transferIpv4(currentPacket, resultBuffer)
          currentPacket.position(row.startPos + dstPos)
          transferIpv4(currentPacket, resultBuffer)

          count += 1
        }
      }
    }
    resultBuffer.putInt(0, count)
    resultBuffer.flip().asInstanceOf[ByteBuffer]
  }

  override def preStart() = {
    connection ! Write(ByteString(toSend), Ack)
  }

  def receiveWithLogging = {
    case Ack =>
      connection ! Write(ByteString(toSend), Ack)
    case PeerClosed =>
      context.stop(self)
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
