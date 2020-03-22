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
package com.pubmatic.spark.sql.execution.datasources.csv

import java.nio.charset.Charset

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import com.pubmatic.spark.internal.Logging
import com.pubmatic.spark.sql.catalyst.InternalRow
import com.pubmatic.spark.sql.catalyst.csv.{CSVOptions, UnivocityGenerator}
import com.pubmatic.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import com.pubmatic.spark.sql.types.StructType

class CsvOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    params: CSVOptions) extends OutputWriter with Logging {

  private val charset = Charset.forName(params.charset)

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), charset)

  private val gen = new UnivocityGenerator(dataSchema, writer, params)

  if (params.headerFlag) {
    gen.writeHeaders()
  }

  override def write(row: InternalRow): Unit = gen.write(row)

  override def close(): Unit = gen.close()
}
