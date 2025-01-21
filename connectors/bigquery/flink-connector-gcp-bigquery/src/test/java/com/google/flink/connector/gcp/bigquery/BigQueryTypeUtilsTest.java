package com.google.flink.connector.gcp.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.google.common.truth.Truth.assertThat;

/** Tests for {@link BigQueryTypeUtils}. */
@RunWith(Parameterized.class)
public class BigQueryTypeUtilsTest {

  @Parameterized.Parameters(name = "BigQuery Field: {0} -> Flink Type: {1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {new TableFieldSchema().setName("string_field").setType("STRING"), DataTypes.STRING()},
          {new TableFieldSchema().setName("int64_field").setType("INTEGER"), DataTypes.BIGINT()},
          {new TableFieldSchema().setName("bool_field").setType("BOOLEAN"), DataTypes.BOOLEAN()},
          {new TableFieldSchema().setName("float64_field").setType("FLOAT"), DataTypes.DOUBLE()},
          {new TableFieldSchema().setName("bytes_field").setType("BYTES"), DataTypes.BYTES()},
          {new TableFieldSchema().setName("date_field").setType("DATE"), DataTypes.DATE()},
          {
              new TableFieldSchema().setName("datetime_field").setType("DATETIME"),
              DataTypes.TIMESTAMP_LTZ()
          },
          {new TableFieldSchema().setName("time_field").setType("TIME"), DataTypes.TIME()},
          {
              new TableFieldSchema().setName("timestamp_field").setType("TIMESTAMP"),
              DataTypes.TIMESTAMP_LTZ()
          },
          {
              new TableFieldSchema().setName("numeric_field").setType("NUMERIC"),
              DataTypes.DECIMAL(38, 9)
          },
          {
              new TableFieldSchema().setName("bignumeric_field").setType("BIGNUMERIC"),
              DataTypes.BYTES()
          },
          {
              new TableFieldSchema()
                  .setName("struct_field")
                  .setType("RECORD")
                  .setFields(
                      Arrays.asList(
                          new TableFieldSchema().setName("nested_string").setType("STRING"),
                          new TableFieldSchema().setName("nested_int64").setType("INTEGER"))),
              DataTypes.ROW(
                  DataTypes.FIELD("nested_string", DataTypes.STRING()),
                  DataTypes.FIELD("nested_int64", DataTypes.BIGINT()))
          },
          {
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
                                      new TableFieldSchema()
                                          .setName("nested_string")
                                          .setType("STRING"),
                                      new TableFieldSchema()
                                          .setName("nested_int64")
                                          .setType("INTEGER"))))),
              DataTypes.ROW(
                  DataTypes.FIELD("top_level_string", DataTypes.STRING()),
                  DataTypes.FIELD(
                      "nested",
                      DataTypes.ROW(
                          DataTypes.FIELD("nested_string", DataTypes.STRING()),
                          DataTypes.FIELD("nested_int64", DataTypes.BIGINT()))))
          },
          {
              new TableFieldSchema()
                  .setName("date_range_field")
                  .setType("RECORD")
                  .setFields(
                      Arrays.asList(
                          new TableFieldSchema().setName("lower").setType("DATE"),
                          new TableFieldSchema().setName("upper").setType("DATE"))),
              DataTypes.ROW(
                  DataTypes.FIELD("lower", DataTypes.DATE()),
                  DataTypes.FIELD("upper", DataTypes.DATE()))
          },
          {
              new TableFieldSchema()
                  .setName("numeric_range_field")
                  .setType("RECORD")
                  .setFields(
                      Arrays.asList(
                          new TableFieldSchema().setName("lower").setType("NUMERIC"),
                          new TableFieldSchema().setName("upper").setType("NUMERIC"))),
              DataTypes.ROW(
                  DataTypes.FIELD("lower", DataTypes.DECIMAL(38, 9)),
                  DataTypes.FIELD("upper", DataTypes.DECIMAL(38, 9)))
          },
          {new TableFieldSchema().setName("json_field").setType("JSON"), DataTypes.STRING()},
          {
              new TableFieldSchema().setName("geography_field").setType("GEOGRAPHY"),
              DataTypes.STRING()
          },
          {new TableFieldSchema().setName("json_data").setType("JSON"), DataTypes.STRING()},
          {
              new TableFieldSchema()
                  .setName("nullable_struct_field")
                  .setType("RECORD")
                  .setFields(
                      Arrays.asList(
                          new TableFieldSchema()
                              .setName("nullable_string")
                              .setType("STRING")
                              .setMode("NULLABLE"),
                          new TableFieldSchema()
                              .setName("required_int64")
                              .setType("INTEGER")
                              .setMode("REQUIRED"))),
              DataTypes.ROW(
                  DataTypes.FIELD("nullable_string", DataTypes.STRING()),
                  DataTypes.FIELD("required_int64", DataTypes.BIGINT()))
          }
        });
  }

  private final TableFieldSchema bigQueryField;
  private final DataType expectedFlinkType;

  public BigQueryTypeUtilsTest(TableFieldSchema bigQueryField, DataType expectedFlinkType) {
    this.bigQueryField = bigQueryField;
    this.expectedFlinkType = expectedFlinkType;
  }

  @Test
  public void testToFlinkType() {
    DataType flinkType = BigQueryTypeUtils.toFlinkType(bigQueryField);
    assertThat(flinkType).isEqualTo(expectedFlinkType);
  }
}