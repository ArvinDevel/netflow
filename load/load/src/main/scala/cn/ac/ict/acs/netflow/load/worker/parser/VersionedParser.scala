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
package cn.ac.ict.acs.netflow.load.worker.parser

import java.nio.ByteBuffer

trait VersionedParser {

  def getVersion: Int

  /**
   * get current netflow package flow count
   * @param data the data contain router ip and netflow data. (single package)
   * @param startPos  start form netflow header
   * @return
   */
  def getFlowCount(data: ByteBuffer, startPos: Int): Int

  /**
   * Get the current package's Unix timestamp
   * @param data the data contain router ip and netflow data. (single package)
   * @param startPos start from netflow header
   * @return
   */
  def getTime(data: ByteBuffer, startPos: Int): Long

  /**
   * Get the current netflow version body position
   * @param startPos start from netflow header
   * @return
   */
  def getBodyPos(startPos: Int): Int

}


/**
 * V9 format
 * --------------header---------------
 * |    version     |   flowSetCount    |
 * |          systemUpTime              |
 * |          unixSeconds               |
 * |        packageSequence             |
 * |             sourceID               |
 * ----------------body---------------
 * |              flowSet               |
 * |              flowSet               |
 * |              ......                |
 * -----------------------------------
 */
object V9Parser extends VersionedParser {
  override def getVersion: Int = 9

  override def getFlowCount(data: ByteBuffer, startPos: Int): Int = data.getShort(startPos + 2)

  override def getTime(data: ByteBuffer, startPos: Int): Long = {
    (data.getInt(startPos + 8) & 0xFFFFFFFFFFL) * 1000
  }
  override def getBodyPos(startPos: Int): Int = startPos + 20

}

/**
 * V5 format
 * ---------------------header-------------------------
 * |        version         |   flowSetCount (1-30)  |
 * |                    sysUpTime                    |
 * |                   Epoch seconds                 |
 * |                   Nano seconds                  |
 * |                   Flows Seen                    |
 * | EngineType | EngineID |      Sampling info      |
 * ----------------------body-------------------------
 * |                    flowSet                      |
 * |                    flowSet                      |
 * |                    ......                       |
 * ---------------------------------------------------
 *
 */
object V5Parser extends VersionedParser {

  val tmpArray = Array(
    (8, 4), (12, 4), (15, 4), (10, 2), (14, 2),
    (2, 4), (1, 4), (22, 4), (21, 4), (7, 2),
    (11, 2), (-1, 1), (6, 1), (4, 1), (5, 1),
    (16, 2), (17, 2), (9, 1), (13, 1), (-1, 2))
  val temp = new Template(5, 20, tmpArray)

  override def getVersion: Int = 5
  override def getFlowCount(data: ByteBuffer, startPos: Int): Int = data.getShort(startPos + 2)
  override def getTime(data: ByteBuffer, startPos: Int): Long = {
    (data.getShort(startPos + 8) & 0xFFFFFFFFFFL) * 1000
  }
  override def getBodyPos(startPos: Int): Int = startPos + 24

}
