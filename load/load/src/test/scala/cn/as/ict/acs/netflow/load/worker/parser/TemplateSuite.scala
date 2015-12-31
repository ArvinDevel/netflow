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
package cn.as.ict.acs.netflow.load.worker.parser

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.{Matchers, FunSuite}

import cn.ac.ict.acs.netflow.load.worker.parser.TemplateKey
import cn.ac.ict.acs.netflow.util.IPUtils

case class TemplateKey2(routerIp: Array[Byte], templateId: Int)

class TemplateSuite extends FunSuite with Matchers {

  test("TemplateKey1 hash & equals") {
    import IPUtils._

    val hm = new ConcurrentHashMap[TemplateKey, Int]
    val key1 = TemplateKey(fromString("192.168.1.1"), 257)
    hm.put(key1, 5)
    val key2 = TemplateKey(fromString("192.168.1.1"), 257)
    assert(hm.get(key2) === 5)
  }

}
