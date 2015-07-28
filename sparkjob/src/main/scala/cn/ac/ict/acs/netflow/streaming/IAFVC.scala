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

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
 * *set inputPath
 * *set iteratorPrecision(times of 3600, 60, 1, .1, .01, or.001)
 * *set compuationPrecision(times of 3600, 60, 1, .1, .01, or.001)
 * *iteratorPrecision must ge(>=) than computationPrecision
 */

case class IAFVC(
    itPrecision: Double = 60.0, coPrecision: Double = 1.0, outPath: String = "", f: Int = -1) {

  /**
   * * configuration parameter
   */
  private val _iteratorPrecision: Double = itPrecision
  private val _computationPrecision: Double = coPrecision
  private val _outputPath: String = outPath
  private val _flag: Int = f

  /**
   * * control parameter
   */
  private var _sourceIP: String = ""
  private var _destinationIP: String = ""
  private var _packetsNumber: Int = 0

  /**
   * * control condition parameter
   */
  private var _timeIterator: Double = 0.0
  private var _timeIteratorRecords = new ArrayBuffer[Double]()
  private var _startTime: Double = 0.0
  private var _currentIndex: Int = 0

  // (sourceIP -> (_destinationIP,_packetsNumber))
  private var _hashMapSD = new HashMap[String, Tuple2[String, Int]]()

  // (_destinationIP -> (number of connections, sum of packet Numebers)
  private var _hashMapDN = new HashMap[String, Tuple2[Int, Int]]()

  private var _iafv: Double = 0.0
  private var _arrayOfIAFVLength = (_iteratorPrecision / _computationPrecision).round.toInt

  private var _arrayOfIAFV = new Array[Double](_arrayOfIAFVLength)

  def initial(): Unit = {

    if (_iteratorPrecision < _computationPrecision) {
      println(" error! iteratorPrecision should not be less than computationPrecision")
      // should quit the execution here!
    }

    _sourceIP = ""
    _destinationIP = ""
    _packetsNumber = 0

    _timeIterator = 0.0
    _timeIteratorRecords = new ArrayBuffer[Double]()
    _startTime = 0.0
    _currentIndex = 0

    _hashMapSD = new HashMap[String, Tuple2[String, Int]]()

    _hashMapDN = new HashMap[String, Tuple2[Int, Int]]()

    _iafv = 0.0
    _arrayOfIAFVLength = (_iteratorPrecision / _computationPrecision).round.toInt + 1

    _arrayOfIAFV = new Array[Double](_arrayOfIAFVLength)
    for (i <- 0 to _arrayOfIAFV.length - 1) {
      _arrayOfIAFV(i) = 0
    }
  }

  def getCorrespondingValue(s: String, v: Double): Double = {

    val tempString = s.split(":")
    var tempIt: Double = 0.0
    var tempIterator: String = ""

    if (v % 3600 == 0) {
      tempIterator = tempString(0)
      tempIt = (tempIterator.toInt) * 3600
    } else if (v % 60 == 0) {
      tempIterator = tempString(1)
      tempIt = ((tempString(0).toInt) * 3600 +
        (tempIterator.toInt) * 60)
    } else {
      val tempS = v.toString
      val tempI1 = tempS.indexOf(".")
      val tempI2 = tempS.length - 1
      if ((tempI2 - tempI1) == 1 && tempS(tempI2) == '0') {
        val tempI3 = tempString(2).indexOf(".")
        for (i <- 0 to tempI3 - 1) {
          tempIterator += tempString(2)(i)
        }
        tempIt = ((tempString(0).toInt) * 3600 +
          (tempString(1)).toInt * 60 +
          tempIterator.toDouble)
      } else {
        val tempI3 = tempString(2).indexOf(".")
        for (i <- 0 to tempI3 + (tempI2 - tempI1)) {
          tempIterator += tempString(2)(i)
        }
        tempIt = ((tempString(0)).toInt * 3600 +
          (tempString(1)).toInt * 60 +
          tempIterator.toDouble)
      }
    }
    return tempIt
  }

  def computeIAFV(): Double = {

    var iafvTemp: Double = 0.0

    _hashMapDN.foreach(k => {
      if (k._2._1 == 1) {
        _hashMapDN -= (k._1)
      }
    })

    val m = _hashMapDN.size
    var sip: Int = 0

    _hashMapDN.foreach(k => {
      sip = sip + k._2._1
    })

    if (m > 0) {
      iafvTemp = (sip - m) * 1.0 / m
    } else {
      iafvTemp = 0.0
    }

    return iafvTemp

  }

  def sortRecords(records: Array[String]): Array[String] = {

    val resultRecords = new Array[String](records.length)

    return resultRecords
  }

  def timeTransform(timeDouble: Double): String = {

    var timeString = ""
    val td = (timeDouble / 3600).intValue
    timeString += td.toString() + ":"
    val td1 = ((timeDouble - td * 3600) / 60).intValue
    timeString += td1.toString() + ":" + (timeDouble - td * 3600 - td1 * 60).toString

    return timeString
  }

  def getFeatures(rdd: RDD[Record]): ArrayBuffer[Array[Double]] = {

    val bufferIAFVS = new ArrayBuffer[Array[Double]]()

    // initialization
    this.initial()

    val records = rdd.collect()
    println(s"===================================RECORD SIZE ==========${records.size}")

    var counter: Int = 0

    if (records.size == 0) {
      return null
    }

    val each = records(0)

    _timeIterator = this.getCorrespondingValue(each.timeString, _iteratorPrecision)
    _timeIteratorRecords += _timeIterator
    _startTime = this.getCorrespondingValue(each.timeString, _computationPrecision)
    _currentIndex = ((_startTime - _timeIterator) / _computationPrecision).round.toInt

    _sourceIP = each.srcIPString
    _destinationIP = each.dstIPString
    _packetsNumber = 1
    _hashMapSD += (_sourceIP -> (_destinationIP, _packetsNumber))
    _hashMapDN += (_destinationIP -> (1, _packetsNumber))

    counter = counter + 1
    /**
     * * start computation
     */

    while (counter < records.size) {

      val line = records(counter)

      /**
       * * for eachline, get the necessary parameters
       */
      val each = line
      val tempInt = this.getCorrespondingValue(each.timeString, _computationPrecision)
      _sourceIP = each.srcIPString
      _destinationIP = each.dstIPString
      _packetsNumber = 1

      /**
       * * if the packet is in this minute, compute and update HashMaps;
       * * else, update iafv value and clear related HashMaps
       * * for each second, just update the HashMaps to calculate
       * * And at the start of each second, clear the HashMaps
       */

      if ((tempInt - _timeIterator) < _iteratorPrecision) {

        if ((tempInt - _startTime) < _computationPrecision) {

          if (_hashMapSD.contains(_sourceIP)) {

            if (_hashMapSD.apply(_sourceIP)._1 != _destinationIP) {

              val destinationDN = _hashMapSD.apply(_sourceIP)._1
              if (_hashMapDN.contains(destinationDN)) {
                val dn = _hashMapDN.apply(destinationDN)._1 - 1
                if (dn == 0) {
                  _hashMapDN -= (destinationDN)
                } else {
                  _hashMapDN.update(destinationDN,
                    (dn, _hashMapDN.apply(destinationDN)._2 - _hashMapSD.apply(_sourceIP)._2))
                }
              }
              _hashMapSD -= (_sourceIP)

            } else if (_hashMapSD.apply(_sourceIP)._1 == _destinationIP) {

              _hashMapSD.update(_sourceIP,
                (_destinationIP, _packetsNumber + _hashMapSD.apply(_sourceIP)._2))
              _hashMapDN.update(_destinationIP, (_hashMapDN.apply(_destinationIP)._1,
                _packetsNumber + _hashMapDN.apply(_destinationIP)._2))
            }

          } else {

            _hashMapSD += (_sourceIP -> (_destinationIP, _packetsNumber))
            if (_hashMapDN.contains(_destinationIP)) {
              val dn = _hashMapDN.apply(_destinationIP)._1 + 1
              _packetsNumber = _packetsNumber + _hashMapDN.apply(_destinationIP)._2
              _hashMapDN += (_destinationIP -> (dn, _packetsNumber))
            } else {
              _hashMapDN += (_destinationIP -> (1, _packetsNumber))
            }

          }

        } else {

          _iafv = this.computeIAFV()
          _arrayOfIAFV(_currentIndex) = _iafv

          _startTime = tempInt
          _currentIndex = ((_startTime - _timeIterator) / _computationPrecision).round.toInt

          _iafv = 0.0
          _hashMapSD.clear()
          _hashMapDN.clear()

          _hashMapSD += (_sourceIP -> (_destinationIP, _packetsNumber))
          _hashMapDN += (_destinationIP -> (1, _packetsNumber))
        }

      } else {

        _iafv = this.computeIAFV()
        _arrayOfIAFV(_currentIndex) = _iafv

        bufferIAFVS += _arrayOfIAFV.clone()

        _timeIterator = this.getCorrespondingValue(each.timeString, _iteratorPrecision)
        _timeIteratorRecords += _timeIterator
        _startTime = tempInt
        _currentIndex = ((_startTime - _timeIterator) / _computationPrecision).round.toInt

        _iafv = 0.0
        for (i <- 0 to _arrayOfIAFV.length - 1) {
          _arrayOfIAFV(i) = 0
        }

        _hashMapSD.clear()
        _hashMapDN.clear()

        _hashMapSD += (_sourceIP -> (_destinationIP, _packetsNumber))
        _hashMapDN += (_destinationIP -> (1, _packetsNumber))

      }
      counter = counter + 1
    }

    /**
     * * calculate the last iafv
     */
    _iafv = this.computeIAFV()

    _arrayOfIAFV(_currentIndex) = _iafv

    bufferIAFVS += _arrayOfIAFV.clone()

    return bufferIAFVS
  }
}
