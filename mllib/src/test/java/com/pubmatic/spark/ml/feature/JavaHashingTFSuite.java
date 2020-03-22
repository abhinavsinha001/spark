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

import org.junit.Assert;
import org.junit.Test;

import com.pubmatic.spark.SharedSparkSession;
import com.pubmatic.spark.mllib.linalg.Vector;
import com.pubmatic.spark.sql.Dataset;
import com.pubmatic.spark.sql.Row;
import com.pubmatic.spark.sql.RowFactory;
import com.pubmatic.spark.sql.types.DataTypes;
import com.pubmatic.spark.sql.types.Metadata;
import com.pubmatic.spark.sql.types.StructField;
import com.pubmatic.spark.sql.types.StructType;


public class JavaHashingTFSuite extends SharedSparkSession {

  @Test
  public void hashingTF() {
    List<Row> data = Arrays.asList(
      RowFactory.create(0.0, "Hi I heard about Spark"),
      RowFactory.create(0.0, "I wish Java could use case classes"),
      RowFactory.create(1.0, "Logistic regression models are neat")
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
    });

    Dataset<Row> sentenceData = spark.createDataFrame(data, schema);
    Tokenizer tokenizer = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words");
    Dataset<Row> wordsData = tokenizer.transform(sentenceData);
    int numFeatures = 20;
    HashingTF hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(numFeatures);
    Dataset<Row> featurizedData = hashingTF.transform(wordsData);
    IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
    IDFModel idfModel = idf.fit(featurizedData);
    Dataset<Row> rescaledData = idfModel.transform(featurizedData);
    for (Row r : rescaledData.select("features", "label").takeAsList(3)) {
      Vector features = r.getAs(0);
      Assert.assertEquals(numFeatures, features.size());
    }
  }
}
