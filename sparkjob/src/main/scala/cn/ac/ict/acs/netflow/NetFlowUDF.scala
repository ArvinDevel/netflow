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
package cn.ac.ict.acs.netflow

import org.apache.spark.sql.SQLContext

abstract class NetFlowUDF[T, RT] {

  def name: String

  def eval(input: T): RT

  def udfRegister(sqlContext: SQLContext): Unit
}

case class IPv4(ip: String) extends NetFlowUDF[String, Array[Byte]] {
  val name = "ipv4"

  private val ba = ip.split('.').map(_.toInt.toByte)

  def eval(input: String) = ba

  def udfRegister(sqlContext: SQLContext) = sqlContext.udf.register(name, eval _)

}
