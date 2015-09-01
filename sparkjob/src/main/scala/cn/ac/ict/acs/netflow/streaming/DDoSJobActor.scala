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

import scala.collection.mutable
import scala.concurrent.Future

import akka.actor.{ActorSelection, Actor, Props}

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkConf

import cn.ac.ict.acs.netflow.{JobMessages, Logging, NetFlowConf}
import cn.ac.ict.acs.netflow.util.{ActorLogReceive, AkkaUtils, Utils}

class DDoSJobActor(loadMasterUrl: String) extends Actor with ActorLogReceive with Logging {
  import context.dispatcher

  import JobMessages._
  var master: ActorSelection = _
  var loaderInfos: Seq[LoadInfo] = _

  override def preStart() = {
    logInfo(s"DDoSJob started with master: $loadMasterUrl to be connected")
    val masterAddress = AkkaUtils.toLMAkkaUrl(loadMasterUrl, AkkaUtils.protocol())
    master = context.actorSelection(masterAddress)
    master ! GetAllLoaders
  }

  def receiveWithLogging = {
    case AllLoadersAvailable(infos) =>
      loaderInfos = infos
      Future { runJob() }
  }

  def runJob(): Unit = {
    val conf = new SparkConf().setAppName("DDoSDetection")
    val ssc = new StreamingContext(conf, Seconds(1))
    val receivers = loaderInfos.map(info => new DDoSReceiver(info.ip, info.streamingPort))
    val allStreams: DStream[Record] = ssc.union(receivers.map(ssc.receiverStream(_)))

    val updateState = (values: Seq[DDoSState], state: Option[DDoSState]) => {
      val stateObj = state.getOrElse(new DDoSState)
      values.size match {
        case 0 => stateObj.expireCounter += 1
        case n => values.foreach(stateObj.merge(_))
      }
      if (stateObj.expireCounter > 5) None else Option(stateObj)
    }

    val localStates: DStream[(Long, DDoSState)] = allStreams.mapPartitions { records =>
      val states = mutable.HashMap.empty[Long, DDoSState]
      records.foreach { record =>
        states.getOrElseUpdate(record.time, new DDoSState).insert(record.srcIp, record.dstIp)
      }
      states.iterator
    }

    val allStates = localStates.updateStateByKey[DDoSState](updateState)
    val finishedStates: DStream[(Long, DDoSState)] = allStates.filter(_._2.expireCounter == 5)

    finishedStates.foreachRDD { rdd =>

    }

    ssc.start()
    ssc.awaitTermination()
  }
}

object DDoSJobActor {

  /**
   * @param args arg0: netflow-load://
   */
  def main(args: Array[String]) {
    val host = Utils.localHostName()
    val (actorSystem, _) =
      AkkaUtils.createActorSystem("DDoSJob", host, 0, new NetFlowConf(false))
    actorSystem.actorOf(Props(classOf[DDoSJobActor], args(0)))
    actorSystem.awaitTermination()
  }
}
