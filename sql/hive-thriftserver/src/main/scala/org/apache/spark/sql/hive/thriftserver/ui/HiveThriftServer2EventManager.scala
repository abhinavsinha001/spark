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

import com.pubmatic.spark.SparkContext
import com.pubmatic.spark.scheduler.SparkListenerEvent

/**
 * This class manages events generated by the thriftserver application. It converts the
 * operation and session events to listener events and post it into the live listener bus.
 */
private[thriftserver] class HiveThriftServer2EventManager(sc: SparkContext) {

  def postLiveListenerBus(event: SparkListenerEvent): Unit = {
    sc.listenerBus.post(event)
  }

  def onSessionCreated(ip: String, sessionId: String, userName: String = "UNKNOWN"): Unit = {
    postLiveListenerBus(SparkListenerThriftServerSessionCreated(ip, sessionId,
      userName, System.currentTimeMillis()))
  }

  def onSessionClosed(sessionId: String): Unit = {
    postLiveListenerBus(SparkListenerThriftServerSessionClosed(sessionId,
      System.currentTimeMillis()))
  }

  def onStatementStart(
      id: String,
      sessionId: String,
      statement: String,
      groupId: String,
      userName: String = "UNKNOWN"): Unit = {
    postLiveListenerBus(SparkListenerThriftServerOperationStart(id, sessionId, statement, groupId,
      System.currentTimeMillis(), userName))
  }

  def onStatementParsed(id: String, executionPlan: String): Unit = {
    postLiveListenerBus(SparkListenerThriftServerOperationParsed(id, executionPlan))
  }

  def onStatementCanceled(id: String): Unit = {
    postLiveListenerBus(SparkListenerThriftServerOperationCanceled(id, System.currentTimeMillis()))
  }

  def onStatementError(id: String, errorMsg: String, errorTrace: String): Unit = {
    postLiveListenerBus(SparkListenerThriftServerOperationError(id, errorMsg, errorTrace,
      System.currentTimeMillis()))
  }

  def onStatementFinish(id: String): Unit = {
    postLiveListenerBus(SparkListenerThriftServerOperationFinish(id, System.currentTimeMillis()))

  }

  def onOperationClosed(id: String): Unit = {
    postLiveListenerBus(SparkListenerThriftServerOperationClosed(id, System.currentTimeMillis()))
  }
}

private[thriftserver] case class SparkListenerThriftServerSessionCreated(
    ip: String,
    sessionId: String,
    userName: String,
    startTime: Long) extends SparkListenerEvent

private[thriftserver] case class SparkListenerThriftServerSessionClosed(
    sessionId: String, finishTime: Long) extends SparkListenerEvent

private[thriftserver] case class SparkListenerThriftServerOperationStart(
    id: String,
    sessionId: String,
    statement: String,
    groupId: String,
    startTime: Long,
    userName: String = "UNKNOWN") extends SparkListenerEvent

private[thriftserver] case class SparkListenerThriftServerOperationParsed(
    id: String,
    executionPlan: String) extends SparkListenerEvent

private[thriftserver] case class SparkListenerThriftServerOperationCanceled(
    id: String, finishTime: Long) extends SparkListenerEvent

private[thriftserver] case class SparkListenerThriftServerOperationError(
    id: String,
    errorMsg: String,
    errorTrace: String,
    finishTime: Long) extends SparkListenerEvent

private[thriftserver] case class SparkListenerThriftServerOperationFinish(
    id: String,
    finishTime: Long) extends SparkListenerEvent

private[thriftserver] case class SparkListenerThriftServerOperationClosed(
    id: String,
    closeTime: Long) extends SparkListenerEvent


