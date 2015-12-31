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

object CodeFormatter {
  def format(code: String): String = new CodeFormatter().addLines(code).result()
}

private class CodeFormatter {
  private val code = new StringBuilder
  private var indentLevel = 0
  private val indentSize = 2
  private var indentString = ""

  private def addLine(line: String): Unit = {
    val indentChange =
      line.count(c => "({".indexOf(c) >= 0) - line.count(c => ")}".indexOf(c) >= 0)
    val newIndentLevel = math.max(0, indentLevel + indentChange)
    // Lines starting with '}' should be de-indented even if they contain '{' after;
    // in addition, lines ending with ':' are typically labels
    val thisLineIndent = if (line.startsWith("}") || line.startsWith(")") || line.endsWith(":")) {
      " " * (indentSize * (indentLevel - 1))
    } else {
      indentString
    }
    code.append(thisLineIndent)
    code.append(line)
    code.append("\n")
    indentLevel = newIndentLevel
    indentString = " " * (indentSize * newIndentLevel)
  }

  private def addLines(code: String): CodeFormatter = {
    code.split('\n').foreach(s => addLine(s.trim()))
    this
  }

  private def result(): String = code.result()
}
