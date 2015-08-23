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

package cn.ac.ict.acs.netflow.load.worker.parquet

import cn.ac.ict.acs.netflow.load.worker.codegen.GenerateFlowSetWriter

import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer

import cn.ac.ict.acs.netflow.Logging
import cn.ac.ict.acs.netflow.load.worker.DataFlowSet


class FlowSetWriteSupport extends WriteSupport[DataFlowSet] with Logging {
  import ParquetSchema._

  private val schema = overallSchema
  private var consumer: RecordConsumer = null

  override def init(configuration: Configuration) = {
    new WriteSupport.WriteContext(schema, new HashMap[String, String].asJava)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer) = {
    consumer = recordConsumer
  }

  override def write(flowSet: DataFlowSet) = {
    val template = flowSet.template
    val writer = GenerateFlowSetWriter.generate(template)
    writer.insert(consumer, flowSet)
  }
}
