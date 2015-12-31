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

package cn.ac.ict.acs.netflow.load.util

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentLinkedQueue}

class ByteBufferPool {

  private val innerStack: ConcurrentLinkedDeque[ByteBuffer] =
    new ConcurrentLinkedDeque[ByteBuffer]()

<<<<<<< HEAD
  // use 1500 bytes, so huge 12.20 Arvin
=======
>>>>>>> 0ff90c74ba797a32bdea0460b0d8f9a1c5175746
  def get(): ByteBuffer = synchronized {
    try {
      innerStack.pop()
    } catch {
      case e: NoSuchElementException =>
        ByteBuffer.allocate(1500)
    }
  }

<<<<<<< HEAD
  // should push before buffer.clear()?
  // cauze ByteBufferPool maintain ByteBuffer to provide to tmp use.
  // it stores just empty byteBuffer. 12.20 Arvin
=======
>>>>>>> 0ff90c74ba797a32bdea0460b0d8f9a1c5175746
  def release(sb: ShareableBuffer): Unit = {
    sb.synchronized {
      if (sb.holderNum.decrementAndGet() == 0) {
        sb.buffer.clear()
        innerStack.push(sb.buffer)
      }
    }
  }

  def shutdown(): Unit = {
    innerStack.clear()
  }
}

case class ShareableBuffer(
    buffer: ByteBuffer,
    holderNum: AtomicInteger = new AtomicInteger(1))
