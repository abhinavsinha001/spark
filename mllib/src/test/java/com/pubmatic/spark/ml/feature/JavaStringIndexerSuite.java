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
import java.util.List;

import static com.pubmatic.spark.sql.types.DataTypes.*;

import org.junit.Assert;
import org.junit.Test;

import com.pubmatic.spark.SharedSparkSession;
import com.pubmatic.spark.sql.Dataset;
import com.pubmatic.spark.sql.Row;
import com.pubmatic.spark.sql.RowFactory;
import com.pubmatic.spark.sql.types.StructField;
import com.pubmatic.spark.sql.types.StructType;

public class JavaStringIndexerSuite extends SharedSparkSession {

  @Test
  public void testStringIndexer() {
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("label", StringType, false)
    });
    List<Row> data = Arrays.asList(
      cr(0, "a"), cr(1, "b"), cr(2, "c"), cr(3, "a"), cr(4, "a"), cr(5, "c"));
    Dataset<Row> dataset = spark.createDataFrame(data, schema);

    StringIndexer indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex");
    Dataset<Row> output = indexer.fit(dataset).transform(dataset);

    Assert.assertEquals(
      Arrays.asList(cr(0, 0.0), cr(1, 2.0), cr(2, 1.0), cr(3, 0.0), cr(4, 0.0), cr(5, 1.0)),
      output.orderBy("id").select("id", "labelIndex").collectAsList());
  }

  /**
   * An alias for RowFactory.create.
   */
  private Row cr(Object... values) {
    return RowFactory.create(values);
  }
}
