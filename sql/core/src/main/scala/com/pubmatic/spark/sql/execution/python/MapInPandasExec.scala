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

package com.pubmatic.spark.sql.execution.python

import scala.collection.JavaConverters._

import com.pubmatic.spark.TaskContext
import com.pubmatic.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import com.pubmatic.spark.rdd.RDD
import com.pubmatic.spark.sql.catalyst.InternalRow
import com.pubmatic.spark.sql.catalyst.expressions._
import com.pubmatic.spark.sql.catalyst.plans.physical._
import com.pubmatic.spark.sql.execution.{SparkPlan, UnaryExecNode}
import com.pubmatic.spark.sql.types.{StructField, StructType}
import com.pubmatic.spark.sql.util.ArrowUtils
import com.pubmatic.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * A relation produced by applying a function that takes an iterator of pandas DataFrames
 * and outputs an iterator of pandas DataFrames.
 *
 * This is somewhat similar with [[FlatMapGroupsInPandasExec]] and
 * `com.pubmatic.spark.sql.catalyst.plans.logical.MapPartitionsInRWithArrow`
 *
 */
case class MapInPandasExec(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode {

  private val pandasFunction = func.asInstanceOf[PythonUDF].func

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private val batchSize = conf.arrowMaxRecordsPerBatch

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { inputIter =>
      // Single function with one struct.
      val argOffsets = Array(Array(0))
      val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
      val sessionLocalTimeZone = conf.sessionLocalTimeZone
      val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)
      val outputTypes = child.schema

      // Here we wrap it via another row so that Python sides understand it
      // as a DataFrame.
      val wrappedIter = inputIter.map(InternalRow(_))

      // DO NOT use iter.grouped(). See BatchIterator.
      val batchIter =
        if (batchSize > 0) new BatchIterator(wrappedIter, batchSize) else Iterator(wrappedIter)

      val context = TaskContext.get()

      val columnarBatchIter = new ArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        argOffsets,
        StructType(StructField("struct", outputTypes) :: Nil),
        sessionLocalTimeZone,
        pythonRunnerConf).compute(batchIter, context.partitionId(), context)

      val unsafeProj = UnsafeProjection.create(output, output)

      columnarBatchIter.flatMap { batch =>
        // Scalar Iterator UDF returns a StructType column in ColumnarBatch, select
        // the children here
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = output.indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())
        flattenedBatch.rowIterator.asScala
      }.map(unsafeProj)
    }
  }
}
