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
package cn.ac.ict.acs.netflow.load.master

import java.nio.ByteBuffer

import cn.ac.ict.acs.netflow.JobMessages.{LoadInfo, AllLoadersAvailable, GetAllLoaders}
import cn.ac.ict.acs.netflow.load.master.CommandSet.CmdStruct
import cn.ac.ict.acs.netflow.metrics.MetricsSystem
import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.remote.RemotingLifecycleEvent
import akka.serialization.SerializationExtension

import cn.ac.ict.acs.netflow._
import cn.ac.ict.acs.netflow.load.{ CombineStatus, LoadMessages }
import cn.ac.ict.acs.netflow.util._
import cn.ac.ict.acs.netflow.ha.{ LeaderElectionAgent, MonarchyLeaderAgent, LeaderElectable }

class LoadMaster(masterHost: String, masterPort: Int, webUiPort: Int, val conf: NetFlowConf)
    extends Actor with ActorLogReceive with LeaderElectable with Logging {

  import DeployMessages._
  import LoadMasterMessages._
  import LoadMessages._
  import MasterMessages._
  import ConfigurationMessages._

  import context.dispatcher

  // the interval for checking workes and receivers
  val WORKER_TIMEOUT = conf.getLong("netflow.LoadWorker.timeout", 60) * 1000
  // Remove a dead worker after given interval
  val REAPER_ITERATIONS = conf.getInt("netflow.dead.worker.persistence", 15)
  // Master recovery mode
  val RECOVERY_MODE = conf.get("netflow.deploy.recoveryMode", "NONE")

  // This may contain dead workers and receivers
  val workers = new mutable.HashSet[LoadWorkerInfo]
  // Current alive workers and receivers
  val idToWorker = new mutable.HashMap[String, LoadWorkerInfo]
  // Current alive workers and receivers
  val addressToWorker = new mutable.HashMap[Address, LoadWorkerInfo]

  Utils.checkHost(masterHost, "Expected hostname")

  val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf)
  val masterSource = new LoadMasterSource(this)

  val loadMasterUrl = "netflow-load://" + masterHost + ":" + masterPort
  var loadMasterWebUIUrl: String = _
  var state = LoadMasterRecoveryState.STANDBY
  var persistenceEngine: MasterPersistenceEngine = _
  var leaderElectionAgent: LeaderElectionAgent = _

  // load master service
  val loadServer = new MasterService(self, conf)

  /**
   * about balance
   */
  // workerIP => (IP,port)
  val workerToPort = new mutable.HashMap[String, (String, Int)]()
  // worker : buffer used rate[0,100]
  val workerToBufferRate = new mutable.HashMap[String, Double]()
  // worker : receiver => 1 : n
  val workerToCollectors = new mutable.HashMap[String, ArrayBuffer[String]]()

  // receiver : worker => 1 : n
  val collectorToWorkers = new mutable.HashMap[String, ArrayBuffer[String]]()

  val workerToStreamingPort = new mutable.HashMap[String, Int]()

  private val halfLimit = 0.5
  private val warnLimit = 0.7

  // combine parquet
  private var combineParquetFinished: Boolean = false

  // when there is no worker registers in cluster,
  // we put the whole request receiver into waitQueue
  val waitQueue = new mutable.HashSet[String]()

  override def preStart(): Unit = {
    logInfo(s"Starting NetFlow LoadMaster at $loadMasterUrl")
    logInfo(s"Running NetFlow version ${cn.ac.ict.acs.netflow.NETFLOW_VERSION}")

    // Listen for remote client disconnection events, since they don't go through Akka's watch()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    // TODO: a pseudo webuiurl here
    loadMasterWebUIUrl = "http://" + masterHost + ":" + webUiPort
    context.system.scheduler.schedule(0.millis, WORKER_TIMEOUT.millis, self, CheckForWorkerTimeOut)

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()

    val (persistenceEngine_, leaderElectionAgent_) =
      RECOVERY_MODE match {
        case "ZOOKEEPER" =>
          logInfo("Persisting recovery state to ZooKeeper")
          val zkFactory =
            new ZKRecoveryModeFactory(conf, SerializationExtension(context.system))
          (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
        case _ =>
          logInfo("No state persisted as a MonarchyLeader")
          (new LoadMasterBHPersistenceEngine(), new MonarchyLeaderAgent(this))
      }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_

    // start the receiver master service
    loadServer.start()
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("LoadMaster actor restarted due to exception", reason)
  }

  override def postStop(): Unit = {
    masterMetricsSystem.report()
    masterMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def appointLeader() = {
    self ! AppointedAsLeader
  }

  override def revokeLeadership() = {
    self ! RevokedLeadership
  }

  override def receiveWithLogging: PartialFunction[Any, Unit] = {

    case AppointedAsLeader =>
      // TODO: dummy placeholder
      state = LoadMasterRecoveryState.ALIVE

    case RevokedLeadership => {
      logError("Leadership has been revoked -- load master shutting down.")
      System.exit(0)
    }

    case RegisterWorker(
        id, workHost, workPort, cores, memory, webUiPort, workerIP, tcpPort, streamingPort) => {
      logInfo("Registering %s %s:%d with %d cores, %s RAM".format(
        id, workHost, workPort, cores, Utils.megabytesToString(memory)))

      if (state == LoadMasterRecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new LoadWorkerInfo(id, workHost, workPort, cores, memory, sender(),
          webUiPort, workerIP, tcpPort, streamingPort)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(loadMasterUrl, loadMasterWebUIUrl)

          pushBGPToWorker(sender())
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register Component at same address: "
            + workerAddress)
        }
      }
    }

    case Heartbeat(workerId) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered component $workerId." +
              " Asking it to re-register.")
            sender ! ReconnectWorker(loadMasterUrl)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case BoundPortsRequest =>
      sender ! BoundPortsResponse(masterPort, webUiPort)

    // message about buffer
    case BuffersWarn(workerIp) =>
      logDebug(s"$workerIp send BuffersWarn message to master.")
    //  adjustCollectorByBuffer(workerIp, sender())

    //    case BufferOverFlow(workerIp) =>
    //      logDebug(s"$workerIp send bufferoverflow message to master.")
    //      adjustCollectorByBuffer(workerIp, sender())

    case BufferSimpleReport(workerIp, usageRate) =>
      logDebug(s"get a simple report $workerIp -> $usageRate.")
      workerToBufferRate.update(workerIp, usageRate)

    case BufferWholeReport(workerIp, usageRate, maxSize, curSize) =>
      logDebug(s"get a whole report $workerIp -> $usageRate ($curSize / $maxSize)")
      workerToBufferRate.update(workerIp, usageRate)
    // TODO save another information (maxSize, curSize)

    // message about combine
    // deal with the combine parquet
    case CloseParquet(fileStamp) =>
      combineParquet(fileStamp)

    case CombineFinished(status) =>
      dealWithCombineMessage(status)

    // message about receiver
    case DeleReceiver(receiverIP) =>
      deleteDeadCollector(receiverIP)

    case DeleWorker(workerIP, port) =>
      deleDeadWorker(workerIP, port, notify = false)

    case RequestWorker(collectorIP) =>
      assignWorker(collectorIP)

    // Forwarding rules and BGP table configuration
    case GetAllRules =>
      sender ! CurrentRules(forwardingRules.iterator.toArray)

    case InsertRules(rule) =>
      sender ! insertForwardingRules(rule)
      notifyRulesToAllCollectors()

    case UpdateSingleRule(ruleId, ruleItem) =>
      sender ! SingleRuleSubstitution(forwardingRules.put(ruleId, ruleItem), ruleItem)
      notifyRulesToAllCollectors()

    case DeleteSingleRule(ruleId) =>
      sender ! DeletedRule(forwardingRules.remove(ruleId))
      notifyRulesToAllCollectors()

    case GetAllLoaders =>
      val infos = ArrayBuffer.empty[LoadInfo]
      workers.foreach(worker => infos += LoadInfo(worker.ip, worker.streamPort))
      sender ! AllLoadersAvailable(infos.toSeq)
  }

  // **********************************************************************************
  // As a Configuration Server

  private val forwardingRules = mutable.HashMap.empty[String, RuleItem]

  def generateInsertionId(): String = (new DateTime).toString(TimeUtils.createFormat)

  def insertForwardingRules(rule: ForwardingRule): ConfigurationMessage = {
    val prefix = generateInsertionId()
    rule.rules.zipWithIndex.foreach {
      case (item, i) =>
        forwardingRules(prefix + "-" + i) = item
    }
    InsertionSuccess(rule.rules.size)
  }

  def pushRuleToCollector(collector: String): Unit = {
    getRuleStr match {
      case Some(cmd) => loadServer.collector2Socket(collector).write(cmd)
      case None =>
    }
  }

  private def notifyRulesToAllCollectors(): Unit = {
    val cmd = getRuleStr.get
    cmd.mark()
    loadServer.collector2Socket.foreach(coll => {
      coll._2.write(cmd)
      cmd.reset()
    })
  }

  private def getRuleStr: Option[ByteBuffer] = {
    if (forwardingRules.isEmpty) return None

    val values = forwardingRules.valuesIterator
    val valueMap = mutable.HashMap.empty[String, Int] // string = "desIP,rate"
    val res_key = new StringBuilder()
    val res_val = new StringBuilder()

    values.foreach(value => {
      val _key = value.routerId.concat(",").concat(value.srcPort.toString)
      val _value = value.destIp.concat(",").concat(value.rate)
      if (valueMap.contains(_value)) {
        res_key.append(_key).append(",").append(valueMap(_value)).append(CmdStruct.inner_delim)
      } else {
        val pos = valueMap.size
        valueMap(_value) = pos
        res_key.append(_key).append(",").append(pos).append(CmdStruct.inner_delim)
      }
    })
    res_key.delete(res_key.lastIndexOf(CmdStruct.inner_delim), res_key.length)
    res_val.delete(res_val.lastIndexOf(CmdStruct.inner_delim), res_key.length)
    Some(CommandSet.resRules(res_key.toString(), res_val.toString()))
  }

  // **********************************************************************************

  private def registerWorker(worker: LoadWorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (with different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker

    deleDeadWorker(worker.ip, notify = true)
    addNewWorker(worker.ip, worker.tcpPort)

    pushBGPToWorker(worker.actor)
    true
  }

  private def removeWorker(worker: LoadWorkerInfo): Unit = {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address
    persistenceEngine.removeWorker(worker)

    // we should tell all the receivers who connected with this
    // dead worker to connect with living worker
    deleDeadWorker(worker.ip, worker.tcpPort, notify = true)

    // redo combine thread on anther worker node.
    dealWithCombineError(worker)
  }

  /**
   *  Check for, and remove, any timed-out workers
   */
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT / 1000))
        removeWorker(worker)
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  // ***********************************************************************************
  /**
   * deal with combine
   */

  // only for combine server, since we can not get the exact load worker threads number
  // in current time base, so we put the task to load worker by listening HDFS directory
  private var curCombieWorker: LoadWorkerInfo = _
  private var fileTimeStamp: Long = 0
  private var nextCombIdx = 0

  private def combineParquet(fileStamp: Long): Unit = {
    if (fileStamp != fileTimeStamp) {
      fileTimeStamp = fileStamp
      sendCombMessage()
      logInfo(s"will combine directory ${load.getPathByTime(fileTimeStamp, conf)}")
    }
  }

  private def dealWithCombineMessage(status: CombineStatus.Value): Unit = {
    status match {
      case CombineStatus.FINISH =>
        combineParquetFinished = true
        logInfo(s"Combine the file ${load.getPathByTime(fileTimeStamp, conf)} completely")

      case CombineStatus.PARTIAL_FINISH =>
        combineParquetFinished = true
        logInfo(s"Combine partial files in directory ${load.getPathByTime(fileTimeStamp, conf)}")

      case CombineStatus.DIRECTORY_NOT_EXIST =>
        logWarning(s"Combine directory ${load.getPathByTime(fileTimeStamp, conf)} failed, " +
          s"for there is no such directory.")

      case CombineStatus.UNKNOWN_DIRECTORY =>
        logWarning(s"Combine directory ${load.getPathByTime(fileTimeStamp, conf)} failed, " +
          s"for the directory structure is not parquet structure.")

      case _ => logError(s"Combine error, cannot combine this directory")
    }
  }

  /**
   * deal with the situation that the worker which is running combine thread,
   * so we should redo the combine thread on another worker node if the dead
   * worker is running combine thread.
   */
  private def dealWithCombineError(deadWorker: LoadWorkerInfo): Unit = {
    if (!combineParquetFinished && deadWorker == curCombieWorker) {
      sendCombMessage()
    }
  }

  // determine a worker to run combine job by order
  private def sendCombMessage(): Unit = {
    if(idToWorker.isEmpty) return
    nextCombIdx = (nextCombIdx + 1) % idToWorker.size
    curCombieWorker = idToWorker.toList(nextCombIdx)._2
    curCombieWorker.actor ! CombineParquet(fileTimeStamp) // tell it to combine the parquets
    logInfo(s"will restart combine directory ${load.getPathByTime(fileTimeStamp, conf)} " +
      s"on ${curCombieWorker.host}")
  }

  // ***********************************************************************************
  /**
   *  only for bgp table
   */
  private val bgpTable = new scala.collection.mutable.HashMap[Int, Array[Byte]]

  private def updateBGP(bgpIds: Array[Int], bgpDatas: Array[Array[Byte]]): Unit = {

    if (bgpTable.isEmpty) {
      idToWorker.valuesIterator
        .foreach(_.actor ! updateBGP(bgpIds, bgpDatas))

      var idx = 0
      while (idx != bgpIds.length) {
        bgpTable(bgpIds(idx)) = bgpDatas(idx)
        idx += 1
      }
      return
    }

    // update exist table
    val _bgpIds = new ArrayBuffer[Int]
    val _bgpDatas = new ArrayBuffer[Array[Byte]]

    var idx = 0
    while (idx != bgpIds.length) {
      if (bgpTable.contains(bgpIds(idx))) {
        val value = bgpTable.get(bgpIds(idx)).get
        if (!(value sameElements bgpDatas(idx))) {
          _bgpIds += bgpIds(idx)
          _bgpDatas += bgpDatas(idx)
          bgpTable(bgpIds(idx)) = bgpDatas(idx)
        }
      } else {
        _bgpIds += bgpIds(idx)
        _bgpDatas += bgpDatas(idx)
        bgpTable(bgpIds(idx)) = bgpDatas(idx)
      }
      idx += 1
    }
    idToWorker.valuesIterator
      .foreach(_.actor ! updateBGP(_bgpIds.toArray[Int], _bgpDatas.toArray[Array[Byte]]))

  }

  private def pushBGPToWorker(workerActor: ActorRef): Unit = {
    if (bgpTable.isEmpty) return

    val bgpIds = new Array[Int](bgpTable.size)
    val bgpDatas = new Array[Array[Byte]](bgpTable.size)
    var idx = 0
    bgpTable.foreach(record => {
      bgpIds(idx) = record._1
      bgpDatas(idx) = record._2
      idx += 1
    })
    workerActor ! updateBGP(bgpIds, bgpDatas)
  }

  // ***********************************************************************************

  private def addConnection(worker: String, collector: String): Unit = {

    workerToCollectors.get(worker) match {
      case Some(collectors) => collectors += collector
      case None =>
        val collectors = new ArrayBuffer[String]
        collectors += collector
        workerToCollectors(worker) = collectors
    }

    collectorToWorkers.get(collector) match {
      case Some(_workers) => _workers += worker
      case None =>
        val workers = new ArrayBuffer[String]
        workers += worker
        collectorToWorkers(collector) = workers
    }
  }

  private def deleConnecton(worker: String, collector: String): Unit = {
    workerToCollectors.get(worker) match {
      case Some(collectors) =>
        val idx = collectors.indexOf(collector)
        if (idx == -1) {
          // has deleted
          require(collectorToWorkers.get(collector).isEmpty
            || collectorToWorkers.get(collector).get.indexOf(worker) == -1,
            s" Since worker2collector does not contain $worker -> $collector," +
              s"collector2worker should not contain $collector -> $worker")
        } else {
          collectors.remove(idx)
          require(collectorToWorkers.get(collector).isDefined,
            s"Since worker2collector contains $worker -> $collector," +
              s"so collector2workers should also contain $collector -> $worker")
          collectorToWorkers.get(collector).get -= worker
        }

      case None =>
        require(collectorToWorkers.get(collector).isEmpty ||
          !collectorToWorkers.get(collector).get.contains(worker),
          s" Since worker2collector does not contain $worker -> $collector," +
            s"collector2worker should not contain $collector -> $worker")
    }
  }

  // ***********************************************************************************
  /**
   * deal with worker
   */

  // called after a worker registered successfully, record workerToUdpPort & workerToCollectors
  private def addNewWorker(workerIP: String, workerPort: Int): Unit = {
    workerToPort += (workerIP -> (workerIP, workerPort))
    workerToBufferRate += (workerIP -> 0)
    _assignWorkerToWaitingCollector()

    // when a worker registered in master, select a receiver to connect with this worker
    def _assignWorkerToWaitingCollector(): Unit = {
      if (waitQueue.isEmpty) {
        logInfo("There is no collector waiting for worker.")
        return
      }

      // selected this current new worker ip
      val cmd = CommandSet.resWorkerIPs(Some(Array(workerToPort.get(workerIP).get)), None)

      while (true) {
        try {
          if (waitQueue.isEmpty) {
            logInfo("All wait Queue has been deal with, there is no collector waiting for worker.")
            return
          }

          val collectorIP = if (waitQueue.contains(workerIP)) workerIP else waitQueue.head

          loadServer.collector2Socket.get(collectorIP) match {
            case Some(socket) =>
              if (socket.isConnected) {
                socket.write(cmd)
                waitQueue.remove(collectorIP)
                addConnection(workerIP, collectorIP)
                return
              } else {
                waitQueue.remove(collectorIP)
                throw new NetFlowException(s"the $collectorIP's socket is closed!")
              }

            case None =>
              waitQueue.remove(collectorIP)
              throw new NetFlowException(s"There is no $collectorIP collector!")
          }
        } catch {
          case e: NetFlowException =>
            logWarning(e.getMessage)
        }
      }
    }
  }

  // Called when the heartbeat is timeout (heartbeat mechanism based)
  // Or called when a receiver request worker list who also assigns a dead worker
  private def deleDeadWorker(workerIP: String, port: Int = 0, notify: Boolean = false): Unit = {

    def _delete(workerIP: String): Unit = {
      workerToCollectors.remove(workerIP) match {
        case Some(collectors) =>
          collectors.foreach(collector => {
            collectorToWorkers(collector) -= workerIP
            if (notify) {
              require(workerToPort.get(workerIP).isDefined,
                s"Now worker2Port should exist deadWorker $workerIP")
              val ip_port = workerToPort.get(workerIP).get
              notifyReceiver(collector, None, Some(Array(ip_port)))
            }
          })
        case None =>
          logInfo(s"The $workerIP worker has been deleted.")
      }

      workerToPort -= workerIP
      workerToBufferRate -= workerIP
    }

    // Since a single worker only has one receiver Server,
    // so 'workerToPort' should at most contain one record about this 'workerIP'.
    workerToPort.get(workerIP) match {
      case Some(deadWorker) =>
        if (port == 0) { // ignore port
          _delete(workerIP)
        } else {
          if (deadWorker.equals((workerIP, port))) {
            _delete(workerIP)
          } else {
            logInfo(s"Expect delete $workerIP:$port worker," +
              s"but now this worker is ${deadWorker._1}:${deadWorker._2}." +
              s"So there mast has something wrong if the load worker does not reboot.")
          }
        }

      case None =>
        logInfo(s"The $workerIP:$port worker has been deleted or does not existed.")
    }
  }

  // ***********************************************************************************
  /**
   * deal with receiver
   */

  // called when a master receives a message about Delete dead collector
  private def deleteDeadCollector(collectorIP: String): Unit = {
    // If the collector is down, the flag can been caught by socketChannel.
    // For workers which is connected with this collector, they also know this flag,
    // so here, we only delete related Struction.
    if (waitQueue.contains(collectorIP)) {
      waitQueue -= collectorIP
      logInfo(s"Remove the collector $collectorIP. ")
      return
    }

    collectorToWorkers.remove(collectorIP) match {
      case Some(relatedWorkers) =>
        relatedWorkers.foreach(_worker => {
          val _collector = workerToCollectors.get(_worker)
          require(_collector.isDefined &&
            _collector.get.contains(collectorIP),
            s"Since collector2workers contain ${collectorIP} -> ${_worker}, " +
              s"So worker2Collectors should contain ${_worker}, but now is NONE.")
          _collector.get -= collectorIP
        })

      case None =>
        logError(s"'deleDeadCollector' method should be called only when the collector is lost," +
          s"so ,for a determined $collectorIP collector, " +
          s"'collectorToWorkers' should have one record at least" +
          s"about this collector, but now, it is Empty")
    }
  }

  private def selectSuitableWorkers(collector: String,
    expectWorkerNum: Int): Option[Array[String]] = {

    val availableWorkers = collectorToWorkers.get(collector) match {
      case Some(_workers) =>
        workerToBufferRate.filterNot(x => _workers.contains(x._1)).toList.sortWith(_._2 < _._2)
      case None =>
        workerToBufferRate.toList.sortWith(_._2 < _._2)
    }

    if (availableWorkers.isEmpty) return None

    val actualLen = Math.min(expectWorkerNum, availableWorkers.length)
    val result = new Array[String](actualLen)
    var idx = 0

    val selfworker = availableWorkers.filter(x => x._1 == collector)
    if (selfworker.nonEmpty) {
      result(idx) = selfworker.head._1
      idx += 1
    }

    for (i <- idx until actualLen if availableWorkers(i)._1 != collector) {
      result(i) = availableWorkers(i)._1
    }
    Some(result)
  }

  // called when a receiver ask for worker's info
  private def assignWorker(collector: String, workerNum: Int = 1): Unit = {

    loadServer.collector2Socket.get(collector) match {
      case Some(socket) =>
        if (!socket.isConnected) {
          logDebug(s" Cannot connect with $collector, for socket dese not connected.")
          return
        }

        selectSuitableWorkers(collector, workerNum) match {
          case Some(workers: Array[String]) =>

            val ip_port = new ArrayBuffer[(String, Int)](workers.length)

            workers.foreach(x =>
              workerToPort.get(x) match {
                case Some(_worker) =>
                  ip_port += _worker
                  addConnection(_worker._1, collector)

                case None =>
                  logError(s"Worker is lost? worker2Port does not contain $x " +
                    s"but worker2Rate does contain?")
                  return
              })
            logDebug(s"current selected ip " +
              s"is ${ip_port.map(x => x._1 + ":" + x._2).mkString("  ")}")
            val cmd = CommandSet.resWorkerIPs(Some(ip_port.toArray[(String, Int)]), None)
            logDebug(s"the cmd is ${cmd.array().slice(2, cmd.limit())}")
            val sendSize = socket.write(cmd)
            logDebug(s"the sent size is ${sendSize}")

          case None =>
            waitQueue += collector
            logWarning(s"There is no available worker to run in cluster.")
        }
      case None =>
        logWarning(s"The $collector Collector is not a effective collector.")
    }
  }

  // ***********************************************************************************
  /**
   * deal with balance
   */

  // notify receiver to change worker
  private def notifyReceiver(receiverHost: String,
    addWorker: Option[Array[(String, Int)]],
    deleWorker: Option[Array[(String, Int)]]): Unit = {

    val res = CommandSet.resWorkerIPs(addWorker, deleWorker)

    loadServer.collector2Socket.get(receiverHost) match {
      case None =>
        logError(s"There is no $receiverHost Receiver in 'collector2Socket'," +
          s"so something must be wrong.")

      case Some(socket) =>
        if (socket.isConnected) {
          socket.write(res)
        } else {
          logError(s"Can not connect with $receiverHost receiver")
        }
    }
  }

  // Called when a worker need to adjust receiver number
  // leave the receiver co-located with worker to be the last one to remove
  private def adjustCollectorByBuffer(workerIP: String, workerActor: ActorRef): Unit = {

    // update buffer info
    idToWorker.values.foreach(x => x.actor ! BufferInfo)

    def underHalfRateStrategy(
      availableWorkers: List[(String, Double)],
      collector: String): Unit = {
      // when there is a available worker who's rate is under buffer rate,
      // we believe that this worker in enough to deal with this work.
      require(availableWorkers.head._2 <= 0.5)
      logDebug(s"Select underHalfRateStrategy, current header rate s ${availableWorkers.head._2}")
      val assignedWorker = availableWorkers.head._1
      addConnection(assignedWorker, collector)
      require(workerToPort.contains(assignedWorker),
        s"work2Rate contains $assignedWorker, " +
          s"so worker2Port should always contains $assignedWorker")
      val addWorker = Array(workerToPort(assignedWorker))
      logDebug(s"Select a worker ${addWorker.head._1}:${addWorker.head._2}")
      notifyReceiver(collector, Some(addWorker), None)
    }

    def underWarnRateStrategy(
      availableWorkers: List[(String, Double)],
      collector: String): Unit = {
      logDebug(s"Select underWarnRateStrategy, current header rate is {availableWorkers.head._2}")
      val adjustSize = Math.min(availableWorkers.size, workerToPort.size / 2)
      val addWorker = new Array[(String, Int)](adjustSize)
      for (i <- 0 until adjustSize) {
        val worker = availableWorkers(i)._1
        addConnection(worker, collector)
        require(workerToPort.contains(worker),
          s"work2Rate contains $worker, " +
            s"so worker2Port should always contains $worker")
        addWorker(i) = workerToPort(worker)
      }
      logDebug(
        s"adjust size is $adjustSize, will connect with ${addWorker.map(_._1).mkString(" ")}")
      notifyReceiver(collector, Some(addWorker), None)
    }

    def selectStrategy(
      availableWorkers: List[(String, Double)],
      collector: String,
      ConnectOneWorker: Boolean): Unit = {
      logDebug(s"Current available worker is ${availableWorkers.head._1}, " +
        s"rate is ${availableWorkers.head._2}")

      availableWorkers.head._2 match {
        case x if x <= halfLimit =>
          underHalfRateStrategy(availableWorkers, collector)
        case x if x <= warnLimit =>
          underWarnRateStrategy(availableWorkers, collector)
        case x => logError(s"Too heavy!")
      }
    }

    def Coll2worker(collector: String): Unit = {
      collectorToWorkers.get(collector) match {
        case None => logError(s"coll2Worker empty? ")
        case Some(_workers) =>
          assert(_workers.nonEmpty,
            s"collector should contain $workerIP at least, but know empty")

          val availableWorkers =
            workerToBufferRate.filterNot(x => _workers.contains(x._1)).toList.sortWith(_._2 < _._2)

          if (_workers.length == 1) {
            assert(_workers.head == workerIP,
              s"collector should contain $workerIP at least, but know ${_workers.head}")
            selectStrategy(availableWorkers, collector, ConnectOneWorker = true)
          } else {
            selectStrategy(availableWorkers, collector, ConnectOneWorker = false)
          }
      }
    }

    workerToCollectors.get(workerIP) match {
      case Some(colls) =>
        colls.foreach(Coll2worker)
      case None => logError(s"worker2Collectors should not be null! ")
    }

    //    // deal with the situation that only one collector connect with this worker
    //    def dealWithSingleConnection(collector: String): Boolean = {
    //      // now, our strategy is split the writing stream on this collector
    //      logDebug(s"Call dealWithSingleConnection, current worker is $workerIP, " +
    //        s"it's connected collector is $collector")
    //
    //      val availableWorkers: List[(String, Double)] =
    //        workerToBufferRate.filterNot(x => x._1 == workerIP).toList.sortWith(_._2 < _._2)
    //      if (availableWorkers.isEmpty) {
    //        logInfo(String.format(
    //          "Total worker number is %d (%s)," +
    //            "which are used by %s collector," +
    //            """so there is no available worker to adjust.""" +
    //            "Only to increase worker's thread.",
    //          workerToPort.size: Integer, workerToPort.keys.mkString(" "), collector))
    //
    //        workerActor ! AdjustThread
    //        return false
    //      }
    //
    //      // add new node to current collector which is select from availableWorkers
    //      selectStrategy(availableWorkers, collector)
    //      true
    //    }
    //
    //    // deal with the situation that only more than one collector connect with this worker
    //    def dealWithMutilConnection(collectors: ArrayBuffer[String]): Unit = {
    //
    //      // first add new connection with that collector
    //      // who only connected with this worker
    //      logDebug(s"[ayscb]Call dealWithSingleConnection, current worker is $workerIP, " +
    //        s"it's connected collector is ${collectors.mkString(",")}")
    //
    //      val orderedcollectors = collectorToWorkers.filter(x => collectors.contains(x))
    //        .toList.sortWith(_._2.size < _._2.size)
    //
    //      logDebug(s"[ayscb]ordered dealWithSingleConnection, current worker is $workerIP, " +
    //        s"it's connected collector is ${orderedcollectors.mkString("<")}")
    //
    //      if (orderedcollectors.head._2.size == 1) {
    //        // only has one connection with worker
    //        // split this collector's stream to mutil-workers
    //        val availableWorkers = workerToBufferRate.filterNot(x => x._1 == workerIP)
    //          .toList.sortWith(_._2 < _._2)
    //
    //        selectStrategy(availableWorkers, orderedcollectors.head._1)
    //      } else {
    //        // all collectors which is connected with this worker
    //        val avgRate = new ArrayBuffer[(String, Double)](collectors.length)
    //        collectors.foreach(coll => {
    //          require(collectorToWorkers.get(coll).isDefined,
    //            s" $coll should connectwith $workerIP")
    //
    //          val workers = collectorToWorkers.get(coll).get
    //          val workersRate = workerToBufferRate.filter(x => workers.contains(x))
    //          var sum = 0.0
    //          workersRate.foreach(rate => sum += rate._2)
    //          avgRate += ((coll, sum / workers.size))
    //        })
    //        val orderedAvg = avgRate.sortWith(_._2 > _._2)
    //
    //        val expectCollector = orderedAvg.head._1
    //        val expectWorkers = collectorToWorkers.get(expectCollector).get
    //        val availableWorkers = workerToBufferRate.filterNot(x => expectWorkers.contains(x))
    //          .toList.sortWith(_._2 < _._2)
    //
    //        selectStrategy(availableWorkers, expectCollector)
    //      }
    //    }
    //
    //    workerToCollectors.get(workerIP) match {
    //      case Some(colls) =>
    //        if (colls.size == 1) {
    //          dealWithSingleConnection(colls.head)
    //        } else {
    //          dealWithMutilConnection(colls)
    //        }
    //
    //      case None => logError(s"worker2Collectors should not be null! ")
    //    }
  }
}

object LoadMaster extends Logging {

  import MasterMessages._

  val systemName = "netflowLoadMaster"
  private val actorName = "LoadMaster"

  def main(argStrings: Array[String]): Unit = {
    SignalLogger.register(log)
    val conf = new NetFlowConf(false)
    val masterArg = new LoadMasterArguments(argStrings, conf)
    val (actorSystem, _, _) =
      startSystemAndActor(masterArg.host, masterArg.port, masterArg.webUiPort, conf)
    actorSystem.awaitTermination()
  }

  /**
   * Start the Master and return a four tuple of:
   * (1) The Master actor system
   * (2) The bound port
   * (3) The web UI bound port
   */

  def startSystemAndActor(
    host: String,
    port: Int,
    webUiPort: Int,
    conf: NetFlowConf): (ActorSystem, Int, Int) = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf)
    val actor = actorSystem.actorOf(
      Props(classOf[LoadMaster], host, boundPort, webUiPort, conf), actorName)
    val timeout = AkkaUtils.askTimeout(conf)
    val portsRequest = actor.ask(BoundPortsRequest)(timeout)
    val portsResponse = Await.result(portsRequest, timeout).asInstanceOf[BoundPortsResponse]
    (actorSystem, boundPort, portsResponse.webUIPort)
  }
}
