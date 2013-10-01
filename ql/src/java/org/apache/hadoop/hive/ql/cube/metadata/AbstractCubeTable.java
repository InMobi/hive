package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.*;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public abstract class AbstractCubeTable implements Named {
  private final String name;
  private final List<FieldSchema> columns;
  private final Map<String, String> properties = new HashMap<String, String>();
  private final double weight;

  protected AbstractCubeTable(String name, List<FieldSchema> columns,
      Map<String, String> props, double weight) {
    this.name = name.toLowerCase();
    this.columns = columns;
    this.weight = weight;
    if (props != null) {
      this.properties.putAll(props);
    }
  }

  protected AbstractCubeTable(Table hiveTable) {
    this.name = hiveTable.getTableName().toLowerCase();
    this.columns = hiveTable.getCols();
    this.properties.putAll(hiveTable.getParameters());
    this.weight = getWeight(getProperties(), getName());
  }

  public abstract CubeTableType getTableType();

  public abstract Set<String> getStorages();

  public Map<String, String> getProperties() {
    return properties;
  }

  public static double getWeight(Map<String, String> properties, String name) {
    String wtStr = properties.get(MetastoreUtil.getCubeTableWeightKey(name));
     return wtStr == null ? 0L : Double.parseDouble(wtStr);
  }

  protected void addProperties() {
    properties.put(MetastoreConstants.TABLE_TYPE_KEY, getTableType().name());
    properties.put(MetastoreUtil.getCubeTableWeightKey(name),
        String.valueOf(weight));
  }

  public String getName() {
    return name;
  }

  public List<FieldSchema> getColumns() {
    return columns;
  }

  public double weight() {
    return weight;
  }

  protected void addColumn(FieldSchema column) throws HiveException {
    if (column == null) {
      throw new NullPointerException("Column cannot be null");
    }
    for (FieldSchema c : columns) {
      if (column.equals(c)) {
        throw new HiveException("Column '" + column.getName() + " " + column.getType()
          + "' already exists in " + name) ;
      }
    }
    columns.add(column);
  }

  protected void addColumns(Collection<FieldSchema> columns) throws HiveException{
    if (columns == null) {
      throw new NullPointerException("Columns cannot be null");
    }
    for (FieldSchema column : columns) {
      addColumn(column);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    AbstractCubeTable other = (AbstractCubeTable) obj;

    if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }
    if (this.getColumns() == null) {
      if (other.getColumns() != null) {
        return false;
      }
    } else {
      if (!this.getColumns().equals(other.getColumns())) {
        return false;
      }
    }
    if (this.weight() != other.weight()) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getName() == null) ? 0 :
      getName().hashCode());
    return result;
  }
}
