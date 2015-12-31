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
package cn.ac.ict.acs.netflow.load.worker.bgp

object BGPRoutingTable {

  // we might want to separate v4 item from v6,
  // if so, split into two methods and call them accordingly.
  def search(dst_addr: Array[Byte]): Array[Any] = {
    pseudo
  }

  def update(tuple: BGPTuple): Unit = ???
  
  val pseudo: Array[Any] = Array(
    "192.168.1.1".getBytes,
    "192.168.1.1".getBytes, null,
    "192.168.2.1".getBytes, null,
    "as_path".getBytes("UTF-8"),
    "community".getBytes("UTF-8"),
    "adjacent_as".getBytes("UTF-8"),
    "self_as".getBytes("UTF-8"))

}

case class BGPTuple(fields: Array[Any])
