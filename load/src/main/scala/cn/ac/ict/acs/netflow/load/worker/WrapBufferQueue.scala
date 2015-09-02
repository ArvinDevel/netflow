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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ TimeUnit, Executors, LinkedBlockingQueue, LinkedBlockingDeque }

import cn.ac.ict.acs.netflow.load.LoadConf
import cn.ac.ict.acs.netflow.load.util.ShareableBuffer
import cn.ac.ict.acs.netflow.{ NetFlowConf, Logging }
import cn.ac.ict.acs.netflow.util.ThreadUtils

object WrapBufferQueue {
  val scheduledThreadPool =
    ThreadUtils.newDaemonScheduledExecutor("WrapBufferQueue-ScheduledExecutor", 4)
  val sampleInterval = 10 // s

  def registerScheduled(wrapBuff: WrapBufferQueue): Unit = {

    def resetFlag(delay: Long, isMasterReport: Boolean) = new Runnable {
      override def run(): Unit = {
        val curSize = wrapBuff.bufferQueue.size()
        if (curSize > wrapBuff.lastQueueSize) { // when the size is increase, reset the flag
          if (isMasterReport) {
            wrapBuff.reportMasterFlag = true
          } else {
            wrapBuff.reportWorkerFlag = true
          }
        }
        wrapBuff.lastQueueSize = curSize
      }
    }

    scheduledThreadPool.scheduleAtFixedRate(
      resetFlag(wrapBuff.reportMasterDelay, isMasterReport = true),
      0, wrapBuff.reportMasterDelay, TimeUnit.SECONDS)

    scheduledThreadPool.scheduleAtFixedRate(
      resetFlag(wrapBuff.reportWorkerDelay, isMasterReport = false),
      0, wrapBuff.reportWorkerDelay, TimeUnit.SECONDS)

    scheduledThreadPool.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        wrapBuff.enqueueRate = 1.0 * wrapBuff.enqueueCount.getAndSet(0) / sampleInterval
        wrapBuff.dequeueRate = 1.0 * wrapBuff.dequeueCount.getAndSet(0) / sampleInterval
      }
    }, 0, sampleInterval, TimeUnit.SECONDS)
  }
}

class WrapBufferQueue(
    val loadBalanceStrategyFunc: () => Unit,
    val sendOverflowMessage: () => Unit,
    val conf: NetFlowConf) extends Logging {

  val maxQueueNum = conf.getInt(LoadConf.QUEUE_MAXPACKAGE_NUM, 1 * 1024 * 1024)
  val warnThreshold = {
    val threshold = conf.getInt(LoadConf.QUEUE_WARN_THRESHOLD, 70)
    if (0 < threshold && threshold < 100) threshold else 70
  }
  private val reportMasterDelay = conf.getInt(LoadConf.REPORT_MASTER_DELAY, 5)
  private val reportWorkerDelay = conf.getInt(LoadConf.REPORT_WORKER_DELAY, 3)

  private val adjustThresholdNum =
    (((warnThreshold - 20) * 1.0 / 100) * maxQueueNum).asInstanceOf[Int]
  private val warnThresholdNum = ((warnThreshold * 1.0 / 100) * maxQueueNum).asInstanceOf[Int]

  private val bufferQueue = new LinkedBlockingQueue[ByteBuffer](maxQueueNum)

  @volatile private var reportMasterFlag: Boolean = false
  @volatile private var reportWorkerFlag: Boolean = false
  @volatile var lastQueueSize: Int = 0
  var enqueueCount: AtomicInteger = new AtomicInteger(0)
  var enqueueRate: Double = 0
  var dequeueCount: AtomicInteger = new AtomicInteger(0)
  var dequeueRate: Double = 0

  WrapBufferQueue.registerScheduled(this) // statistic rate by Daemon thread

  private var _previousPacket: ShareableBuffer = null
  private var _currentPacket: ShareableBuffer = null

  class LockLockLock

  val lock = new LockLockLock

  /**
   * User of this method should guarantee not to change the buffer's state
   * since it is shared with another parquet writer thread
   * @return
   */
  def currentPacket: ShareableBuffer = {
    lock.synchronized {
      while (_currentPacket == null || _previousPacket == _currentPacket) {
        lock.wait()
      }
    }
    _previousPacket = _currentPacket
    _currentPacket
  }

  // get the element from queue, block when the queue is empty
  def take(): ShareableBuffer = {
    val data = ShareableBuffer(bufferQueue.take())
    dequeueCount.addAndGet(data.buffer.capacity())
    _currentPacket = data
    lock.synchronized {
      lock.notify()
    }
    data
  }

  // put the element to queue, block when the queue is full
  def put(byteBuffer: ByteBuffer): Unit = {
    checkThreshold()
    bufferQueue.put(byteBuffer)
    enqueueCount.addAndGet(byteBuffer.capacity())
  }

  def currSize = bufferQueue.size()
  def currUsageRate(): Double = 1.0 * bufferQueue.size() / maxQueueNum

  private def checkThreshold(): Unit = {

    val curSize = bufferQueue.size()
    if (reportMasterFlag && curSize > warnThresholdNum) {
      sendOverflowMessage() // will block.....
      reportMasterFlag = false
      return
    }

    if (reportWorkerFlag && curSize > adjustThresholdNum) {
      loadBalanceStrategyFunc()
      reportWorkerFlag = false
      return
    }
  }
}
