package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public final class ColumnMeasure extends CubeMeasure {
  public ColumnMeasure(FieldSchema column, String formatString,
      String aggregate, String unit) {
    this(column, formatString, aggregate, unit, null, null, null);
  }

  public ColumnMeasure(FieldSchema column) {
    this(column, null, null, null);
  }

  public ColumnMeasure(FieldSchema column, String formatString,
    String aggregate, String unit, Date startTime, Date endTime, Double cost) {
   super(column, formatString, aggregate, unit, startTime, endTime, cost) ;
  }

  public ColumnMeasure(String name, Map<String, String> props) {
    super(name, props);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return true;
  }
}
