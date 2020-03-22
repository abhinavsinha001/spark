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

package com.pubmatic.spark.sql.execution.streaming.continuous

import scala.util.control.NonFatal

import com.pubmatic.spark.SparkException
import com.pubmatic.spark.internal.Logging
import com.pubmatic.spark.rdd.RDD
import com.pubmatic.spark.sql.catalyst.InternalRow
import com.pubmatic.spark.sql.catalyst.expressions.Attribute
import com.pubmatic.spark.sql.connector.write.PhysicalWriteInfoImpl
import com.pubmatic.spark.sql.connector.write.streaming.StreamingWrite
import com.pubmatic.spark.sql.execution.{SparkPlan, UnaryExecNode}
import com.pubmatic.spark.sql.execution.streaming.StreamExecution

/**
 * The physical plan for writing data into a continuous processing [[StreamingWrite]].
 */
case class WriteToContinuousDataSourceExec(write: StreamingWrite, query: SparkPlan)
  extends UnaryExecNode with Logging {

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val queryRdd = query.execute()
    val writerFactory = write.createStreamingWriterFactory(
      PhysicalWriteInfoImpl(queryRdd.getNumPartitions))
    val rdd = new ContinuousWriteRDD(queryRdd, writerFactory)

    logInfo(s"Start processing data source write support: $write. " +
      s"The input RDD has ${rdd.partitions.length} partitions.")
    EpochCoordinatorRef.get(
      sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
      sparkContext.env)
      .askSync[Unit](SetWriterPartitions(rdd.getNumPartitions))

    try {
      // Force the RDD to run so continuous processing starts; no data is actually being collected
      // to the driver, as ContinuousWriteRDD outputs nothing.
      rdd.collect()
    } catch {
      case _: InterruptedException =>
        // Interruption is how continuous queries are ended, so accept and ignore the exception.
      case cause: Throwable =>
        cause match {
          // Do not wrap interruption exceptions that will be handled by streaming specially.
          case _ if StreamExecution.isInterruptionException(cause, sparkContext) => throw cause
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw new SparkException("Writing job aborted.", e)
          case _ => throw cause
        }
    }

    sparkContext.emptyRDD
  }
}
