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

package com.pubmatic.spark.serializer

import com.pubmatic.spark.{SparkConf, SparkFunSuite}
import com.pubmatic.spark.LocalSparkContext._
import com.pubmatic.spark.SparkContext
import com.pubmatic.spark.SparkException
import com.pubmatic.spark.internal.config._
import com.pubmatic.spark.internal.config.Kryo._

class KryoSerializerResizableOutputSuite extends SparkFunSuite {

  // trial and error showed this will not serialize with 1mb buffer
  val x = (1 to 400000).toArray

  test("kryo without resizable output buffer should fail on large array") {
    val conf = new SparkConf(false)
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(KRYO_SERIALIZER_BUFFER_SIZE.key, "1m")
    conf.set(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "1m")
    withSpark(new SparkContext("local", "test", conf)) { sc =>
      intercept[SparkException](sc.parallelize(x).collect())
    }
  }

  test("kryo with resizable output buffer should succeed on large array") {
    val conf = new SparkConf(false)
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    conf.set(KRYO_SERIALIZER_BUFFER_SIZE.key, "1m")
    conf.set(KRYO_SERIALIZER_MAX_BUFFER_SIZE.key, "2m")
    withSpark(new SparkContext("local", "test", conf)) { sc =>
      assert(sc.parallelize(x).collect() === x)
    }
  }
}
