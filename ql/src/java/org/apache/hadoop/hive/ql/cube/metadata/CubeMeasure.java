package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public abstract class CubeMeasure extends CubeColumn {
  private final String formatString;
  private final String aggregate;
  private final String unit;
  private final FieldSchema column;

  protected CubeMeasure(FieldSchema column, String formatString,
      String aggregate, String unit) {
    this(column, formatString, aggregate, unit, null, null, null);
  }

  protected CubeMeasure(FieldSchema column, String formatString,
      String aggregate, String unit, Date startTime, Date endTime, Double cost){
    super(column.getName(), startTime, endTime, cost);
    this.column = column;
    assert (column != null);
    assert (column.getName() != null);
    assert (column.getType() != null);
    this.formatString = formatString;
    this.aggregate = aggregate;
    this.unit = unit;
  }

  protected CubeMeasure(String name, Map<String, String> props) {
    super(name, props);
    this.column = new FieldSchema(name,
        props.get(MetastoreUtil.getMeasureTypePropertyKey(name)), "");
    this.formatString = props.get(MetastoreUtil.getMeasureFormatPropertyKey(name));
    this.aggregate = props.get(MetastoreUtil.getMeasureAggrPropertyKey(name));
    this.unit = props.get(MetastoreUtil.getMeasureUnitPropertyKey(name));
  }

  public String getFormatString() {
    return formatString;
  }

  public String getAggregate() {
    return aggregate;
  }

  public String getUnit() {
    return unit;
  }

  public FieldSchema getColumn() {
    return column;
  }

  @Override
  public String getName() {
    return column.getName();
  }

  public String getType() {
    return column.getType();
  }

  @Override
  public String toString() {
    String str = super.toString() + ":" + getType();
    if (unit != null) {
      str += ",unit:" + unit;
    }
    if (aggregate != null) {
      str += ",aggregate:" + aggregate;
    }
    if (formatString != null) {
      str += ",formatString:" + formatString;
    }
    return str;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CubeMeasure other = (CubeMeasure) obj;
    if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (!this.getType().equalsIgnoreCase(other.getType())) {
      return false;
    }
    if (this.getUnit() == null) {
      if (other.getUnit() != null) {
        return false;
      }
    } else if (!this.getUnit().equalsIgnoreCase(other.getUnit())) {
      return false;
    }
    if (this.getAggregate() == null) {
      if (other.getAggregate() != null) {
        return false;
      }
    } else if (!this.getAggregate().equalsIgnoreCase(other.getAggregate())) {
      return false;
    }
    if (this.getFormatString() == null) {
      if (other.getFormatString() != null) {
        return false;
      }
    } else if (!this.getFormatString().equalsIgnoreCase(
        other.getFormatString())) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getType() == null) ? 0 :
        getType().toLowerCase().hashCode());
    result = prime * result + ((unit == null) ? 0 :
        unit.toLowerCase().hashCode());
    result = prime * result + ((aggregate == null) ? 0 :
        aggregate.toLowerCase().hashCode());
    result = prime * result + ((formatString == null) ? 0 :
        formatString.toLowerCase().hashCode());
    return result;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getMeasureClassPropertyKey(getName()),
        getClass().getName());
    props.put(MetastoreUtil.getMeasureTypePropertyKey(getName()), getType());
    if (unit != null) {
      props.put(MetastoreUtil.getMeasureUnitPropertyKey(getName()), unit);
    }
    if (getFormatString() != null) {
      props.put(MetastoreUtil.getMeasureFormatPropertyKey(getName()),
          formatString);
    }
    if (aggregate != null) {
      props.put(MetastoreUtil.getMeasureAggrPropertyKey(getName()), aggregate);
    }
  }
}
