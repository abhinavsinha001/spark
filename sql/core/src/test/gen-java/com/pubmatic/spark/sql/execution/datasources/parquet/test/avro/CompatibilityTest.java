/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.pubmatic.spark.sql.execution.datasources.parquet.test.avro;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public interface CompatibilityTest {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"CompatibilityTest\",\"namespace\":\"com.pubmatic.spark.sql.execution.datasources.parquet.test.avro\",\"types\":[{\"type\":\"enum\",\"name\":\"Suit\",\"symbols\":[\"SPADES\",\"HEARTS\",\"DIAMONDS\",\"CLUBS\"]},{\"type\":\"record\",\"name\":\"ParquetEnum\",\"fields\":[{\"name\":\"suit\",\"type\":\"Suit\"}]},{\"type\":\"record\",\"name\":\"Nested\",\"fields\":[{\"name\":\"nested_ints_column\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"nested_string_column\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]},{\"type\":\"record\",\"name\":\"AvroPrimitives\",\"fields\":[{\"name\":\"bool_column\",\"type\":\"boolean\"},{\"name\":\"int_column\",\"type\":\"int\"},{\"name\":\"long_column\",\"type\":\"long\"},{\"name\":\"float_column\",\"type\":\"float\"},{\"name\":\"double_column\",\"type\":\"double\"},{\"name\":\"binary_column\",\"type\":\"bytes\"},{\"name\":\"string_column\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}]},{\"type\":\"record\",\"name\":\"AvroOptionalPrimitives\",\"fields\":[{\"name\":\"maybe_bool_column\",\"type\":[\"null\",\"boolean\"]},{\"name\":\"maybe_int_column\",\"type\":[\"null\",\"int\"]},{\"name\":\"maybe_long_column\",\"type\":[\"null\",\"long\"]},{\"name\":\"maybe_float_column\",\"type\":[\"null\",\"float\"]},{\"name\":\"maybe_double_column\",\"type\":[\"null\",\"double\"]},{\"name\":\"maybe_binary_column\",\"type\":[\"null\",\"bytes\"]},{\"name\":\"maybe_string_column\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]}]},{\"type\":\"record\",\"name\":\"AvroNonNullableArrays\",\"fields\":[{\"name\":\"strings_column\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"maybe_ints_column\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"int\"}]}]},{\"type\":\"record\",\"name\":\"AvroArrayOfArray\",\"fields\":[{\"name\":\"int_arrays_column\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"int\"}}}]},{\"type\":\"record\",\"name\":\"AvroMapOfArray\",\"fields\":[{\"name\":\"string_to_ints_column\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"int\"},\"avro.java.string\":\"String\"}}]},{\"type\":\"record\",\"name\":\"ParquetAvroCompat\",\"fields\":[{\"name\":\"strings_column\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"string_to_int_column\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"complex_column\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"Nested\"},\"avro.java.string\":\"String\"}}]}],\"messages\":{}}");

  @SuppressWarnings("all")
  public interface Callback extends CompatibilityTest {
    public static final org.apache.avro.Protocol PROTOCOL = com.pubmatic.spark.sql.execution.datasources.parquet.test.avro.CompatibilityTest.PROTOCOL;
  }
}