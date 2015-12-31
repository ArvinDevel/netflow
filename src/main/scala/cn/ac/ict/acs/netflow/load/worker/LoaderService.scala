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

import java.util

import akka.actor.ActorRef
import cn.ac.ict.acs.netflow.load.util.ByteBufferPool
import cn.ac.ict.acs.netflow.load.worker.parser.PacketParser._
import cn.ac.ict.acs.netflow.{NetFlowException, Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.load.worker.parquet.ParquetWriterWrapper
import cn.ac.ict.acs.netflow.util.ThreadUtils
import scala.collection._

<<<<<<< HEAD
// ParquetWriterWrapper with schedule Parquet Writer again, every time slot with one Parquet Writer.
// cauze parquetwriterwrapper is not single instance class, there will be several wrapper, and
// each wrapper schedule several parquet writer?
=======
>>>>>>> 0ff90c74ba797a32bdea0460b0d8f9a1c5175746
final class LoaderService(
    private val workerActor: ActorRef,
    private val bufferList: WrapBufferQueue,
    private val bufferPool: ByteBufferPool,
    private val conf: NetFlowConf) extends Logging {

  private val writerThreadPool = ThreadUtils.newDaemonCachedThreadPool("loadService-writerPool")

  private val writeThreadRate = new mutable.HashMap[Thread, Double]()
  private val RateCount = new java.util.concurrent.atomic.AtomicInteger()

  @volatile private var readRateFlag = false

  def initParquetWriterPool(threadNum: Int) = {
    for (i <- 0 until threadNum) {
      writerThreadPool.submit(newWriter)
    }
  }

  def curThreadsNum: Int = writerThreadPool.getPoolSize

  def adjustWritersNum(newThreadNum: Int) = {

    val _curThreadsNum = curThreadsNum
    logInfo(s"Current total resolving thread number is ${_curThreadsNum}, " +
      s" and will be adjust to $newThreadNum ")

<<<<<<< HEAD


=======
>>>>>>> 0ff90c74ba797a32bdea0460b0d8f9a1c5175746
    if (newThreadNum > _curThreadsNum) {
      // increase threads
      for (i <- 0 until (newThreadNum - _curThreadsNum))
        writerThreadPool.submit(newWriter)
    } else {
      writeThreadRate.synchronized({
        writeThreadRate.keySet.take(_curThreadsNum - newThreadNum)
          .foreach(thread => {
            writeThreadRate -= thread
            thread.interrupt()
          })
      })
    }
  }

  def stopAllWriterThreads() = {
    logInfo(("current threads number is %d, all " +
      "threads will be stopped").format(curThreadsNum))
    writeThreadRate.synchronized({
      writeThreadRate.keySet.foreach(_.interrupt())
      writeThreadRate.clear()
    })
    writerThreadPool.shutdown()
  }

  def curPoolRate: util.ArrayList[Double] = {
    readRateFlag = true
    // get all threads rates
    while (RateCount.get() != curThreadsNum) { Thread.sleep(1000) }

    val list = new util.ArrayList[Double]()
    writeThreadRate.synchronized({
      writeThreadRate.valuesIterator.foreach(list.add)
    })
    readRateFlag = false
    RateCount.set(0)
    list
  }

  /**
   * A new thread to write the parquet file
   * @return
   */
  private def newWriter: Runnable = {

    val writer = new Runnable() {
      private var startTime: Long = 0L
      private var packageCount: Int = 0

      private var hasRead: Boolean = false

      // write data to parquet
      private val writer = new ParquetWriterWrapper(workerActor, conf)

      // num/ms
<<<<<<< HEAD
      // this statistic just calculate number/time 12.22 Arvin
=======
>>>>>>> 0ff90c74ba797a32bdea0460b0d8f9a1c5175746
      private def getCurrentRate: Double = {
        val rate = 1.0 * packageCount / (System.currentTimeMillis() - startTime)
        startTime = System.currentTimeMillis()
        packageCount = 0
        rate
      }

      override def run(): Unit = {
        logInfo("Start sub Write Parquet %d"
          .format(Thread.currentThread().getId))

        writeThreadRate(Thread.currentThread()) = 0.0

        startTime = System.currentTimeMillis()
        packageCount = 0

        try {
          while (true) {
            val data = bufferList.take // block when this list is empty
            if (readRateFlag && !hasRead) {
              writeThreadRate(Thread.currentThread()) = getCurrentRate

              RateCount.incrementAndGet()
              hasRead = true
            }

            // reset 'hasRead'
            if (!readRateFlag & hasRead) {
              hasRead = false
            }

            packageCount += 1
            val (flowSets, packetTime) = parse(data.buffer.duplicate())
            while (flowSets.hasNext) {
              val fs = flowSets.next()
<<<<<<< HEAD
              // just store flowSet, ignore header,
              writer.write(fs, packetTime)// and flowSet has recorded time. Arvin
=======
              writer.write(fs, packetTime)
>>>>>>> 0ff90c74ba797a32bdea0460b0d8f9a1c5175746
            }
            bufferPool.release(data)
          }
        } catch {
          case e: InterruptedException =>
            logError(e.getMessage)
            e.printStackTrace()
          case e: Exception =>
            logError(e.getMessage)
            e.printStackTrace()
          case e: NetFlowException =>
            logError(e.getMessage)
            e.printStackTrace()
        } finally {
          writer.close()
          logError("load server is closed!!!")
        }
      }
    }
    writer
  }
}
