package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Date;
import java.util.Map;

public abstract class CubeDimension extends CubeColumn {

  public CubeDimension(String name) {
    this(name, null, null, null);
  }

  public CubeDimension(String name, Date startTime, Date endTime, Double cost) {
    super(name, startTime, endTime, cost);
  }

  public CubeDimension(String name, Map<String, String> props) {
    super(name, props);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimensionClassPropertyKey(getName()),
        getClass().getName());
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

}
