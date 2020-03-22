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
package com.pubmatic.spark.sql.execution.datasources.json

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import com.pubmatic.spark.internal.Logging
import com.pubmatic.spark.sql.catalyst.InternalRow
import com.pubmatic.spark.sql.catalyst.json.{JacksonGenerator, JSONOptions, JSONOptionsInRead}
import com.pubmatic.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import com.pubmatic.spark.sql.types.StructType

class JsonOutputWriter(
    path: String,
    options: JSONOptions,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with Logging {

  private val encoding = options.encoding match {
    case Some(charsetName) => Charset.forName(charsetName)
    case None => StandardCharsets.UTF_8
  }

  if (JSONOptionsInRead.blacklist.contains(encoding)) {
    logWarning(s"The JSON file ($path) was written in the encoding ${encoding.displayName()}" +
      " which can be read back by Spark only if multiLine is enabled.")
  }

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path), encoding)

  // create the Generator without separator inserted between 2 records
  private[this] val gen = new JacksonGenerator(dataSchema, writer, options)

  override def write(row: InternalRow): Unit = {
    gen.write(row)
    gen.writeLineEnding()
  }

  override def close(): Unit = {
    gen.close()
    writer.close()
  }
}
