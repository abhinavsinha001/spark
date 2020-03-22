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

package com.pubmatic.spark.mllib.linalg.distributed;

import java.util.Arrays;

import org.junit.Test;

import com.pubmatic.spark.SharedSparkSession;
import com.pubmatic.spark.api.java.JavaRDD;
import com.pubmatic.spark.mllib.linalg.Matrix;
import com.pubmatic.spark.mllib.linalg.QRDecomposition;
import com.pubmatic.spark.mllib.linalg.Vector;
import com.pubmatic.spark.mllib.linalg.Vectors;

public class JavaRowMatrixSuite extends SharedSparkSession {

  @Test
  public void rowMatrixQRDecomposition() {
    Vector v1 = Vectors.dense(1.0, 10.0, 100.0);
    Vector v2 = Vectors.dense(2.0, 20.0, 200.0);
    Vector v3 = Vectors.dense(3.0, 30.0, 300.0);

    JavaRDD<Vector> rows = jsc.parallelize(Arrays.asList(v1, v2, v3), 1);
    RowMatrix mat = new RowMatrix(rows.rdd());

    QRDecomposition<RowMatrix, Matrix> result = mat.tallSkinnyQR(true);
  }
}
