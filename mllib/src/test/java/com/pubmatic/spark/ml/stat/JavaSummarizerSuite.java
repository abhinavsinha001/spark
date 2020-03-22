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

package com.pubmatic.spark.ml.stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import com.pubmatic.spark.SharedSparkSession;
import com.pubmatic.spark.sql.Row;
import com.pubmatic.spark.sql.Dataset;
import static com.pubmatic.spark.sql.functions.col;
import com.pubmatic.spark.ml.feature.LabeledPoint;
import com.pubmatic.spark.ml.linalg.Vector;
import com.pubmatic.spark.ml.linalg.Vectors;

public class JavaSummarizerSuite extends SharedSparkSession {

  private transient Dataset<Row> dataset;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    List<LabeledPoint> points = new ArrayList<>();
    points.add(new LabeledPoint(0.0, Vectors.dense(1.0, 2.0)));
    points.add(new LabeledPoint(0.0, Vectors.dense(3.0, 4.0)));

    dataset = spark.createDataFrame(jsc.parallelize(points, 2), LabeledPoint.class);
  }

  @Test
  public void testSummarizer() {
    dataset.select(col("features"));
    Row result = dataset
      .select(Summarizer.metrics("mean", "max", "count").summary(col("features")))
      .first().getStruct(0);
    Vector meanVec = result.getAs("mean");
    Vector maxVec = result.getAs("max");
    long count = result.getAs("count");

    assertEquals(2L, count);
    assertArrayEquals(new double[]{2.0, 3.0}, meanVec.toArray(), 0.0);
    assertArrayEquals(new double[]{3.0, 4.0}, maxVec.toArray(), 0.0);
  }
}
