package org.apache.spark.ml.evaluation

/**
 * @author ：ljf
 * @date ：2020/11/18 15:04
 * @description：add evaluateAll method
 * @modified By：
 * @version: $ 1.0
 */
/*
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

import com.esotericsoftware.kryo.serializers.VersionFieldSerializer.Since
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.Dataset

/**
 * :: DeveloperApi ::
 * Abstract class for evaluators that compute metrics from predictions.
 */
abstract class BDAEvaluator extends Params {

  /**
   * Evaluates model output and returns a scalar metric.
   * The value of [[isLargerBetter]] specifies whether larger values are better.
   *
   * @param dataset  a dataset that contains labels/observations and predictions.
   * @param paramMap parameter map that specifies the input columns and output metrics
   * @return metric
   */
  def evaluate(dataset: Dataset[_], paramMap: ParamMap): Double = {
    this.copy(paramMap).evaluate(dataset)
  }

  /**
   * Evaluates model output and returns a scalar metric.
   * The value of [[isLargerBetter]] specifies whether larger values are better.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @return metric
   */
  def evaluate(dataset: Dataset[_]): Double

  /**
   * Indicates whether the metric returned by `evaluate` should be maximized (true, default)
   * or minimized (false).
   * A given evaluator may support multiple metrics which may be maximized or minimized.
   */
  def isLargerBetter: Boolean = true

  override def copy(extra: ParamMap): BDAEvaluator

  def evaluateAll(dataset: Dataset[_]): Map[String, Double]

  def getMetricName: String
}
