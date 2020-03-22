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

package org.apache.spark.sql.hive.thriftserver.ui

import com.pubmatic.spark.SparkConf
import com.pubmatic.spark.scheduler.SparkListener
import com.pubmatic.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}
import com.pubmatic.spark.ui.SparkUI

class HiveThriftServer2HistoryServerPlugin extends AppHistoryServerPlugin {

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new HiveThriftServer2Listener(store, conf, None, false))
  }

  override def setupUI(ui: SparkUI): Unit = {
    val store = new HiveThriftServer2AppStatusStore(ui.store.store)
    if (store.getSessionCount > 0) {
      new ThriftServerTab(store, ui)
    }
  }

  override def displayOrder: Int = 1
}

