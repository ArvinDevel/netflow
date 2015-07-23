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

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}


object SStreamingTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("customized_receiver")
    val ssc = new StreamingContext(conf, Minutes(1))
    val netflowStream = ssc.receiverStream(new DDoSReceiver("10.30.1.154", 52318))
    netflowStream.foreachRDD { rdd =>

      // create case class iafv


      println("====================")
      println(rdd.count())
      println("====================")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
