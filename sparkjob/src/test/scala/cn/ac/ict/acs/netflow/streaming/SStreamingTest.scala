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

import java.nio.ByteBuffer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SStreamingTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("customized_receiver")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
    val optionClassification = "SVM"
    val train = DDoSModel.getTrainData(sc,"/Users/yijie/code/netflow/dev/newtrain")
    val modelSVM = DDoSModel.getSVMModel(train)
    val modelTree = DDoSModel.getTreeModel(train)

    val netflowPacketStream = ssc.receiverStream(new DDoSReceiver("10.30.1.154", 56658))
    netflowPacketStream.foreachRDD( rdd => {
      println("-----------------" + rdd.count() + "--------------")
    })

    val updateState = (values: Seq[DDoSState], state: Option[DDoSState]) => {
      val stateObj = state.getOrElse(new DDoSState)
      values.size match {
        case 0 => stateObj.expireCounter += 1
        case n => values.foreach(stateObj.merge(_))
      }
      if (stateObj.expireCounter > 5) None else Option(stateObj)
    }

    val localStates: DStream[(Long, DDoSState)] = netflowPacketStream.mapPartitions { packetIter =>
      val states = mutable.HashMap.empty[Long, DDoSState]
      packetIter.foreach { packet =>
        val count = packet.count
        val time = packet.time
        val data = ByteBuffer.wrap(packet.data)
        var i = 0
        while (i < count) {
          val srcIp = new Array[Byte](4)
          val dstIp = new Array[Byte](4)
          data.get(srcIp)
          val src = ByteBuffer.wrap(srcIp)
          data.get(dstIp)
          val dst = ByteBuffer.wrap(dstIp)

          states.getOrElseUpdate(time, new DDoSState).insert(src, dst)

          i += 1
        }
      }
      states.iterator
    }

    val allStates = localStates.updateStateByKey[DDoSState](updateState)
    val finishedStates: DStream[(Long, DDoSState)] = allStates.filter(_._2.expireCounter == 5)

    finishedStates.foreachRDD { rdd =>
      println("---------------------")
      rdd.foreachPartition { partitionRecords =>
        partitionRecords.foreach { case (key,value) =>
          val feature = Vectors.dense(value.build())
          optionClassification match {
            case "SVM" =>
              val prediction = modelSVM.predict(feature)
              if(prediction == 1.0){
                println(key + ":" + value.output(0))
              }
            case "Tree" =>
              val prediction = modelTree.predict(feature)
              if(prediction == 1.0){
                println(key + ":" + value.output(0))
              }
          }
        }
      }
    }

    ssc.checkpoint("hdfs://localhost:9000/netflow/DDoS/_checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}