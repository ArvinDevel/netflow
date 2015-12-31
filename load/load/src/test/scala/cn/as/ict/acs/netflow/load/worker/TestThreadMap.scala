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
package cn.as.ict.acs.netflow.load.worker

import scala.collection.mutable

/**
  * Created by ayscb on 6/29/15.
  */
object TestThreadMap {

  val writeThreadRate = new mutable.HashMap[Thread, String]()

  def t : Thread = {
    new Thread( new Runnable {
      //   writeThreadRate(Thread.currentThread()) =
      // Thread.currentThread().getName + Thread.currentThread().getId
      override def run(): Unit = {
        writeThreadRate(Thread.currentThread()) =
          Thread.currentThread().getName + Thread.currentThread().getId
        var i = 10
        while(true){
          writeThreadRate(Thread.currentThread()) =
            writeThreadRate(
              Thread.currentThread()).concat(" -- " + i.toString)
          //   assert(Thread.currentThread().getName + Thread.currentThread().
          // getId == writeThreadRate(Thread.currentThread()))
          println(Thread.currentThread().getName + Thread.currentThread().getId +
            "---->" + writeThreadRate(Thread.currentThread()))
          Thread.sleep(500)
          i -= 1
        }
      }
    })
  }

  def main(args: Array[String]) {
    for(i <-0 to 100){
      t.start()
      Thread.sleep(300)
    }
  }

}
