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

package com.pubmatic.spark.sql.catalyst.util

import java.util.Locale

/**
 * Builds a map in which keys are case insensitive. Input map can be accessed for cases where
 * case-sensitive information is required. The primary constructor is marked private to avoid
 * nested case-insensitive map creation, otherwise the keys in the original map will become
 * case-insensitive in this scenario.
 * Note: CaseInsensitiveMap is serializable. However, after transformation, e.g. `filterKeys()`,
 *       it may become not serializable.
 */
class CaseInsensitiveMap[T] private (val originalMap: Map[String, T]) extends Map[String, T]
  with Serializable {

  //  Note: this class supports Scala 2.13. A parallel source tree has a 2.12 implementation.

  val keyLowerCasedMap = originalMap.map(kv => kv.copy(_1 = kv._1.toLowerCase(Locale.ROOT)))

  override def get(k: String): Option[T] = keyLowerCasedMap.get(k.toLowerCase(Locale.ROOT))

  override def contains(k: String): Boolean =
    keyLowerCasedMap.contains(k.toLowerCase(Locale.ROOT))

  override def updated[B1 >: T](key: String, value: B1): Map[String, B1] = {
    new CaseInsensitiveMap[B1](originalMap + (key -> value))
  }

  override def iterator: Iterator[(String, T)] = keyLowerCasedMap.iterator

  override def removed(key: String): Map[String, T] = {
    new CaseInsensitiveMap(originalMap.filter(!_._1.equalsIgnoreCase(key)))
  }
}

object CaseInsensitiveMap {
  def apply[T](params: Map[String, T]): CaseInsensitiveMap[T] = params match {
    case caseSensitiveMap: CaseInsensitiveMap[T] => caseSensitiveMap
    case _ => new CaseInsensitiveMap(params)
  }
}

