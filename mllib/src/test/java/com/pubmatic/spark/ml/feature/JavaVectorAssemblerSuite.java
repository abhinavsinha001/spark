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

package com.pubmatic.spark.ml.feature;

import java.util.Arrays;

import static com.pubmatic.spark.sql.types.DataTypes.*;

import org.junit.Assert;
import org.junit.Test;

import com.pubmatic.spark.SharedSparkSession;
import com.pubmatic.spark.mllib.linalg.Vector;
import com.pubmatic.spark.mllib.linalg.VectorUDT;
import com.pubmatic.spark.mllib.linalg.Vectors;
import com.pubmatic.spark.sql.Dataset;
import com.pubmatic.spark.sql.Row;
import com.pubmatic.spark.sql.RowFactory;
import com.pubmatic.spark.sql.types.StructField;
import com.pubmatic.spark.sql.types.StructType;

public class JavaVectorAssemblerSuite extends SharedSparkSession {

  @Test
  public void testVectorAssembler() {
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("x", DoubleType, false),
      createStructField("y", new VectorUDT(), false),
      createStructField("name", StringType, false),
      createStructField("z", new VectorUDT(), false),
      createStructField("n", LongType, false)
    });
    Row row = RowFactory.create(
      0, 0.0, Vectors.dense(1.0, 2.0), "a",
      Vectors.sparse(2, new int[]{1}, new double[]{3.0}), 10L);
    Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row), schema);
    VectorAssembler assembler = new VectorAssembler()
      .setInputCols(new String[]{"x", "y", "z", "n"})
      .setOutputCol("features");
    Dataset<Row> output = assembler.transform(dataset);
    Assert.assertEquals(
      Vectors.sparse(6, new int[]{1, 2, 4, 5}, new double[]{1.0, 2.0, 3.0, 10.0}),
      output.select("features").first().<Vector>getAs(0));
  }
}
