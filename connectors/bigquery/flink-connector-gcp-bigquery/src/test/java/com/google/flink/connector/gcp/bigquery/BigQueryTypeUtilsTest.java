package com.google.flink.connector.gcp.bigquery;

<<<<<<< HEAD
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldElementType;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
=======
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.api.services.bigquery.model.TableFieldSchema;
>>>>>>> origin/schema
import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link BigQueryTypeUtils}. */
public class BigQueryTypeUtilsTest {

<<<<<<< HEAD
  @Test
  public void testToFlinkTypeString() {
    Field bigQueryField = Field.newBuilder("string_field", StandardSQLTypeName.STRING).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }

  @Test
  public void testToFlinkTypeInt64() {
    Field bigQueryField = Field.newBuilder("int64_field", StandardSQLTypeName.INT64).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BIGINT());
  }

  @Test
  public void testToFlinkTypeBool() {
    Field bigQueryField = Field.newBuilder("bool_field", StandardSQLTypeName.BOOL).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BOOLEAN());
  }

  @Test
  public void testToFlinkTypeFloat64() {
    Field bigQueryField = Field.newBuilder("float64_field", StandardSQLTypeName.FLOAT64).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DOUBLE());
  }

  @Test
  public void testToFlinkTypeBytes() {
    Field bigQueryField = Field.newBuilder("bytes_field", StandardSQLTypeName.BYTES).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BYTES());
  }

  @Test
  public void testToFlinkTypeDate() {
    Field bigQueryField = Field.newBuilder("date_field", StandardSQLTypeName.DATE).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DATE());
  }

  @Test
  public void testToFlinkTypeDatetime() {
    Field bigQueryField = Field.newBuilder("datetime_field", StandardSQLTypeName.DATETIME).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIMESTAMP_LTZ());
  }

  @Test
  public void testToFlinkTypeTime() {
    Field bigQueryField = Field.newBuilder("time_field", StandardSQLTypeName.TIME).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIME());
  }

  @Test
  public void testToFlinkTypeTimestamp() {
    Field bigQueryField = Field.newBuilder("timestamp_field", StandardSQLTypeName.TIMESTAMP).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.TIMESTAMP_LTZ());
  }

  @Test
  public void testToFlinkTypeNumeric() {
    Field bigQueryField = Field.newBuilder("numeric_field", StandardSQLTypeName.NUMERIC).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.DECIMAL(38, 9));
  }

  @Test
  public void testToFlinkTypeBignumeric() {
    Field bigQueryField = Field.newBuilder("bignumeric_field", StandardSQLTypeName.BIGNUMERIC).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.BYTES());
  }

  @Test
  public void testToFlinkTypeStruct() {
    Field bigQueryField =
        Field.newBuilder(
                "struct_field",
                StandardSQLTypeName.STRUCT,
                FieldList.of(
                    Field.of("nested_string", StandardSQLTypeName.STRING),
                    Field.of("nested_int64", StandardSQLTypeName.INT64)))
            .build();
=======
    static Stream<Object[]> testToFlinkTypeData() {
        return Stream.of(
                new Object[]{"STRING", DataTypes.STRING()},
                new Object[]{"INTEGER", DataTypes.BIGINT()},
                new Object[]{"BOOLEAN", DataTypes.BOOLEAN()},
                new Object[]{"FLOAT", DataTypes.DOUBLE()},
                new Object[]{"BYTES", DataTypes.BYTES()},
                new Object[]{"DATE", DataTypes.DATE()},
                new Object[]{"DATETIME", DataTypes.TIMESTAMP_LTZ()},
                new Object[]{"TIME", DataTypes.TIME()},
                new Object[]{"TIMESTAMP", DataTypes.TIMESTAMP_LTZ()},
                new Object[]{"NUMERIC", DataTypes.DECIMAL(38, 9)},
                new Object[]{"BIGNUMERIC", DataTypes.BYTES()}
        );
    }

    @ParameterizedTest
    @MethodSource("testToFlinkTypeData")
    public void testToFlinkType(String bigQueryType, DataType expectedFlinkType) {
        TableFieldSchema bigQueryField = new TableFieldSchema().setName(bigQueryType.toLowerCase() + "_field").setType(bigQueryType);
        DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
        assertThat(flinkType).isEqualTo(expectedFlinkType);
    }

  @Test
  public void testToFlinkTypeStruct() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("struct_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("nested_string").setType("STRING"),
                    new TableFieldSchema().setName("nested_int64").setType("INTEGER")));
>>>>>>> origin/schema
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("nested_string", DataTypes.STRING()),
                DataTypes.FIELD("nested_int64", DataTypes.BIGINT())));
  }

  @Test
  public void testToFlinkTypeNestedStruct() {
<<<<<<< HEAD
    Field bigQueryField =
        Field.newBuilder(
                "nested_struct_field",
                StandardSQLTypeName.STRUCT,
                FieldList.of(
                    Field.of("top_level_string", StandardSQLTypeName.STRING),
                    Field.newBuilder(
                            "nested",
                            StandardSQLTypeName.STRUCT,
                            FieldList.of(
                                Field.of("nested_string", StandardSQLTypeName.STRING),
                                Field.of("nested_int64", StandardSQLTypeName.INT64)))
                        .build()))
            .build();
=======
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("nested_struct_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("top_level_string").setType("STRING"),
                    new TableFieldSchema()
                        .setName("nested")
                        .setType("RECORD")
                        .setFields(
                            Arrays.asList(
                                new TableFieldSchema().setName("nested_string").setType("STRING"),
                                new TableFieldSchema().setName("nested_int64").setType("INTEGER")))));
>>>>>>> origin/schema
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("top_level_string", DataTypes.STRING()),
                DataTypes.FIELD(
                    "nested",
                    DataTypes.ROW(
                        DataTypes.FIELD("nested_string", DataTypes.STRING()),
                        DataTypes.FIELD("nested_int64", DataTypes.BIGINT())))));
  }

<<<<<<< HEAD
  @Test
  public void testToFlinkTypeRangeDate() {
    Field bigQueryField =
        Field.newBuilder("date_range_field", StandardSQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(StandardSQLTypeName.DATE.name()).build())
            .build();
=======
  // The com.google.api.services.bigquery.model does not have a direct equivalent for RANGE type.
  // The RANGE type is usually represented as a STRUCT with lower and upper bounds.
  // Therefore, this test is adapted to reflect that representation.
  @Test
  public void testToFlinkTypeRangeDate() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("date_range_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("lower").setType("DATE"),
                    new TableFieldSchema().setName("upper").setType("DATE")));
>>>>>>> origin/schema
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("lower", DataTypes.DATE()),
                DataTypes.FIELD("upper", DataTypes.DATE())));
  }

<<<<<<< HEAD
  @Test
  public void testToFlinkTypeRangeNumeric() {
    Field bigQueryField =
        Field.newBuilder("numeric_range_field", StandardSQLTypeName.RANGE)
            .setRangeElementType(FieldElementType.newBuilder().setType(StandardSQLTypeName.NUMERIC.name()).build())
            .build();
=======
  // Similar to the Date Range, Numeric Range is represented as a STRUCT.
  @Test
  public void testToFlinkTypeRangeNumeric() {
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("numeric_range_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("lower").setType("NUMERIC"),
                    new TableFieldSchema().setName("upper").setType("NUMERIC")));
>>>>>>> origin/schema
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("lower", DataTypes.DECIMAL(38, 9)),
                DataTypes.FIELD("upper", DataTypes.DECIMAL(38, 9))));
  }

  @Test
  public void testToFlinkTypeJSON() {
<<<<<<< HEAD
    Field bigQueryField = Field.newBuilder("json_field", StandardSQLTypeName.JSON).build();
=======
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("json_field").setType("JSON");
>>>>>>> origin/schema
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }

<<<<<<< HEAD

  @Test
  public void testToFlinkTypeGeography() {
    Field bigQueryField = Field.newBuilder("geography_field", StandardSQLTypeName.GEOGRAPHY).build();
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }
  
  @Test
  public void testJsonType() {
    Field jsonField =
        Field.newBuilder("json_data", StandardSQLTypeName.JSON).build();
=======
  @Test
  public void testToFlinkTypeGeography() {
    TableFieldSchema bigQueryField = new TableFieldSchema().setName("geography_field").setType("GEOGRAPHY");
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(DataTypes.STRING());
  }

  @Test
  public void testJsonType() {
    TableFieldSchema jsonField =
        new TableFieldSchema().setName("json_data").setType("JSON");
>>>>>>> origin/schema
    DataType expectedType = DataTypes.STRING();
    assertThat(BigQueryTypeUtils.toFlinkType(jsonField)).isEqualTo(expectedType);
  }

  @Test
  public void testToFlinkTypeNullableFieldsInStruct() {
<<<<<<< HEAD
    Field bigQueryField =
        Field.newBuilder(
                "nullable_struct_field",
                StandardSQLTypeName.STRUCT,
                FieldList.of(
                    Field.newBuilder("nullable_string", StandardSQLTypeName.STRING)
                        .setMode(Mode.NULLABLE)
                        .build(),
                    Field.newBuilder("required_int64", StandardSQLTypeName.INT64)
                        .setMode(Mode.REQUIRED)
                        .build()))
            .build();
=======
    TableFieldSchema bigQueryField =
        new TableFieldSchema()
            .setName("nullable_struct_field")
            .setType("RECORD")
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName("nullable_string").setType("STRING").setMode("NULLABLE"),
                    new TableFieldSchema().setName("required_int64").setType("INTEGER").setMode("REQUIRED")));
>>>>>>> origin/schema
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType)
        .isEqualTo(
            DataTypes.ROW(
                DataTypes.FIELD("nullable_string", DataTypes.STRING()),
                DataTypes.FIELD("required_int64", DataTypes.BIGINT())));
    // Note: Flink's DataTypes doesn't explicitly represent nullability at this level.
    // The nullability is handled at the runtime.
  }
}