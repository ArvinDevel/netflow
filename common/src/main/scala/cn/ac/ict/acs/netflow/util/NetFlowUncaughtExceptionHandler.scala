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
package cn.ac.ict.acs.netflow.util

import cn.ac.ict.acs.netflow.Logging

object NetFlowUncaughtExceptionHandler extends Thread.UncaughtExceptionHandler with Logging {

  override def uncaughtException(thread: Thread, exception: Throwable) {
    try {
      logError("Uncaught exception in thread " + thread, exception)

      // We may have been called from a shutdown hook. If so, we must not call System.exit().
      // (If we do, we will deadlock.)
      if (!Utils.inShutdown()) {
        if (exception.isInstanceOf[OutOfMemoryError]) {
          System.exit(NetFlowExitCode.OOM)
        } else {
          System.exit(NetFlowExitCode.UNCAUGHT_EXCEPTION)
        }
      }
    } catch {
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(NetFlowExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(NetFlowExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable) {
    uncaughtException(Thread.currentThread(), exception)
  }
}
