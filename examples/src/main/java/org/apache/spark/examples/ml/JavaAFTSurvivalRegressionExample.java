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

package org.apache.spark.examples.ml;

// $example on$
import java.util.Arrays;
import java.util.List;

import com.pubmatic.spark.ml.regression.AFTSurvivalRegression;
import com.pubmatic.spark.ml.regression.AFTSurvivalRegressionModel;
import com.pubmatic.spark.ml.linalg.VectorUDT;
import com.pubmatic.spark.ml.linalg.Vectors;
import com.pubmatic.spark.sql.Dataset;
import com.pubmatic.spark.sql.Row;
import com.pubmatic.spark.sql.RowFactory;
import com.pubmatic.spark.sql.SparkSession;
import com.pubmatic.spark.sql.types.DataTypes;
import com.pubmatic.spark.sql.types.Metadata;
import com.pubmatic.spark.sql.types.StructField;
import com.pubmatic.spark.sql.types.StructType;
// $example off$

/**
 * An example demonstrating AFTSurvivalRegression.
 * Run with
 * <pre>
 * bin/run-example ml.JavaAFTSurvivalRegressionExample
 * </pre>
 */
public class JavaAFTSurvivalRegressionExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaAFTSurvivalRegressionExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(1.218, 1.0, Vectors.dense(1.560, -0.605)),
      RowFactory.create(2.949, 0.0, Vectors.dense(0.346, 2.158)),
      RowFactory.create(3.627, 0.0, Vectors.dense(1.380, 0.231)),
      RowFactory.create(0.273, 1.0, Vectors.dense(0.520, 1.151)),
      RowFactory.create(4.199, 0.0, Vectors.dense(0.795, -0.226))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("censor", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty())
    });
    Dataset<Row> training = spark.createDataFrame(data, schema);
    double[] quantileProbabilities = new double[]{0.3, 0.6};
    AFTSurvivalRegression aft = new AFTSurvivalRegression()
      .setQuantileProbabilities(quantileProbabilities)
      .setQuantilesCol("quantiles");

    AFTSurvivalRegressionModel model = aft.fit(training);

    // Print the coefficients, intercept and scale parameter for AFT survival regression
    System.out.println("Coefficients: " + model.coefficients());
    System.out.println("Intercept: " + model.intercept());
    System.out.println("Scale: " + model.scale());
    model.transform(training).show(false);
    // $example off$

    spark.stop();
  }
}
