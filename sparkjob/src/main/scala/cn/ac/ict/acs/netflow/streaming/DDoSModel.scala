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

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

/**
 * Created by netflow on 9/8/15.
 */
object DDoSModel {
  def getTrainData(sc:SparkContext,trainPath:String):RDD[LabeledPoint] = {

    val trainData = sc.textFile(trainPath)
    trainData.map { line =>
      val parts = line.split(" ")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts.tail.map(x => x.toDouble)))
    }
  }

  def getSVMModel(train:RDD[LabeledPoint]):SVMModel = {

    val numIterations = 20
    SVMWithSGD.train(train, numIterations)
  }

  def getTreeModel(train:RDD[LabeledPoint]):DecisionTreeModel = {

    val numClasses =100
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    DecisionTree.trainClassifier(train, numClasses,categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
  }
}
