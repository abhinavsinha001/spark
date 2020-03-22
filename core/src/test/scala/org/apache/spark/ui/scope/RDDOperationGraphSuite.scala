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

package com.pubmatic.spark.ui.scope

import com.pubmatic.spark.SparkFunSuite

class RDDOperationGraphSuite extends SparkFunSuite {
  test("Test simple cluster equals") {
    // create a 2-cluster chain with a child
    val c1 = new RDDOperationCluster("1", false, "Bender")
    val c2 = new RDDOperationCluster("2", false, "Hal")
    c1.attachChildCluster(c2)
    c1.attachChildNode(new RDDOperationNode(3, "Marvin", false, false, "collect!"))

    // create an equal cluster, but without the child node
    val c1copy = new RDDOperationCluster("1", false, "Bender")
    val c2copy = new RDDOperationCluster("2", false, "Hal")
    c1copy.attachChildCluster(c2copy)

    assert(c1 == c1copy)
  }
}
