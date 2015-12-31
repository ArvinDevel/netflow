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
package cn.as.ict.acs.netflow.load.worker.parquet

import cn.ac.ict.acs.netflow.{load, NetFlowConf}

/**
 * Created by ayscb on 6/29/15.
 */
object TestParquetWriterWrapper {

  val conf = new NetFlowConf()

  private val dicInterValTime = load.dirCreationInterval(conf)
  private val closeDelay = load.writerCloseDelay(conf)

  def getDelayTime(timeStamp: Long): Long = {
    dicInterValTime - (timeStamp - load.getTimeBase(timeStamp, conf) ) + closeDelay
  }

  def main(args: Array[String]) {

    println(dicInterValTime)
    println(closeDelay)
    for( i <- 0 to 10){
      val cuT = System.currentTimeMillis()
      val tm = cuT + i * 1000

      println(tm + "  ---> " + getDelayTime(tm)  + " --> " + load.getTimeBase(tm, conf))
    }
  }
}
