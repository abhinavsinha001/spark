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
package com.pubmatic.spark.sql.execution.datasources.v2.text

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import com.pubmatic.spark.sql.AnalysisException
import com.pubmatic.spark.sql.catalyst.util.CompressionCodecs
import com.pubmatic.spark.sql.execution.datasources.{CodecStreams, OutputWriter, OutputWriterFactory}
import com.pubmatic.spark.sql.execution.datasources.text.{TextOptions, TextOutputWriter}
import com.pubmatic.spark.sql.execution.datasources.v2.FileWriteBuilder
import com.pubmatic.spark.sql.internal.SQLConf
import com.pubmatic.spark.sql.types._
import com.pubmatic.spark.sql.util.CaseInsensitiveStringMap

class TextWriteBuilder(
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    formatName: String,
    supportsDataType: DataType => Boolean)
  extends FileWriteBuilder(options, paths, formatName, supportsDataType) {
  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 1) {
      throw new AnalysisException(
        s"Text data source supports only a single column, and you have ${schema.size} columns.")
    }
  }

  override def prepareWrite(
      sqlConf: SQLConf,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)

    val textOptions = new TextOptions(options)
    val conf = job.getConfiguration

    textOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new TextOutputWriter(path, dataSchema, textOptions.lineSeparatorInWrite, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".txt" + CodecStreams.getCompressionExtension(context)
      }
    }
  }
}
