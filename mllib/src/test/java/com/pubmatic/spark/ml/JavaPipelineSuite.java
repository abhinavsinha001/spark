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

package com.pubmatic.spark.ml;

import java.io.IOException;

import org.junit.Test;

import com.pubmatic.spark.SharedSparkSession;
import com.pubmatic.spark.api.java.JavaRDD;
import com.pubmatic.spark.ml.classification.LogisticRegression;
import static com.pubmatic.spark.ml.classification.LogisticRegressionSuite.generateLogisticInputAsList;
import com.pubmatic.spark.ml.feature.LabeledPoint;
import com.pubmatic.spark.ml.feature.StandardScaler;
import com.pubmatic.spark.sql.Dataset;
import com.pubmatic.spark.sql.Row;

/**
 * Test Pipeline construction and fitting in Java.
 */
public class JavaPipelineSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    JavaRDD<LabeledPoint> points =
      jsc.parallelize(generateLogisticInputAsList(1.0, 1.0, 100, 42), 2);
    dataset = spark.createDataFrame(points, LabeledPoint.class);
  }

  @Test
  public void pipeline() {
    StandardScaler scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures");
    LogisticRegression lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures");
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[]{scaler, lr});
    PipelineModel model = pipeline.fit(dataset);
    model.transform(dataset).createOrReplaceTempView("prediction");
    Dataset<Row> predictions = spark.sql("SELECT label, probability, prediction FROM prediction");
    predictions.collectAsList();
  }
}
