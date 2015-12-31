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
package cn.ac.ict.acs.netflow.load.worker.codegen

import java.nio.ByteBuffer

import com.google.common.cache.{CacheLoader, CacheBuilder}
import org.apache.parquet.io.api.{Binary, RecordConsumer}
import org.codehaus.janino.ClassBodyEvaluator

import cn.ac.ict.acs.netflow.Logging
import cn.ac.ict.acs.netflow.load.worker.DataFlowSet
import cn.ac.ict.acs.netflow.load.worker.parser.Template

class CodeGenContext {
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  def freshName(prefix: String): String = {
    s"$prefix${curId.getAndIncrement}"
  }
}

abstract class GeneratedClass {
  def generate(template: Template): Any
}

abstract class CodeGenerator[InType <: AnyRef, OutType <: AnyRef] extends Logging {
  protected val flowSetType: String = classOf[DataFlowSet].getName
  protected val templateType: String = classOf[Template].getName


  protected def create(in: InType): OutType

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  protected def compile(code: String): GeneratedClass = {
    cache.get(code)
  }

  /**
   * Compile the Java source code into a Java class, using Janino.
   */
  private[this] def doCompile(code: String): GeneratedClass = {
    val evaluator = new ClassBodyEvaluator()
    evaluator.setParentClassLoader(getClass.getClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName("cn.ac.ict.acs.netflow.load.worker.GeneratedClass")
    evaluator.setDefaultImports(Array(
      classOf[DataFlowSet].getName,
      classOf[RecordConsumer].getName,
      classOf[Binary].getName,
      classOf[ByteBuffer].getName
    ))
    evaluator.setExtendedClass(classOf[GeneratedClass])
    try {
      evaluator.cook(code)
    } catch {
      case e: Exception =>
        val msg = s"failed to compile: $e\n" + CodeFormatter.format(code)
        logError(msg, e)
        throw new Exception(msg, e)
    }
    evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass]
  }

  /**
   * A cache of generated classes.
   *
   * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
   * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
   * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
   * automatically, in order to constrain its memory footprint.  Note that this cache does not use
   * weak keys/values and thus does not respond to memory pressure.
   */
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(100)
    .build(
      new CacheLoader[String, GeneratedClass]() {
        override def load(code: String): GeneratedClass = {
          val startTime = System.nanoTime()
          val result = doCompile(code)
          val endTime = System.nanoTime()
          def timeMs: Double = (endTime - startTime).toDouble / 1000000
          logInfo(s"Code generated in $timeMs ms")
          result
        }
      })

  def generate(template: InType): OutType = create(template)

  /**
   * Create a new codegen context for expression evaluator, used to store those
   * expressions that don't support codegen
   */
  def newCodeGenContext(): CodeGenContext = {
    new CodeGenContext
  }
}
