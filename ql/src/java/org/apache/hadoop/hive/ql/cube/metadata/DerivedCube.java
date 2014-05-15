package org.apache.hadoop.hive.ql.cube.metadata;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.sun.xml.internal.ws.util.StringUtils;

public class DerivedCube extends AbstractCubeTable implements CubeInterface {

  private static final List<FieldSchema> columns = new ArrayList<FieldSchema>();
  static {
    columns.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  private final Cube parent;
  private final Set<String> measures;
  private final Set<String> dimensions;

  public DerivedCube(String name, Set<String> measures,
      Set<String> dimensions, Cube parent) {
    this(name, measures, dimensions, new HashMap<String, String>(), 0L, parent);
  }

  public DerivedCube(String name, Set<String> measures,
      Set<String> dimensions, Map<String, String> properties,
      double weight, Cube parent) {
    super(name, columns, properties, weight);
    this.measures = measures;
    this.dimensions = dimensions;
    this.parent = parent;

    addProperties();
  }

  public DerivedCube(Table tbl, Cube parent) {
    super(tbl);
    this.measures = getMeasures(getName(), getProperties());
    this.dimensions = getDimensions(getName(), getProperties());
    this.parent = parent;
  }

  private Set<CubeMeasure> cachedMeasures;
  private Set<CubeDimension> cachedDims;
  public Set<CubeMeasure> getMeasures() {
    return cachedMeasures;
  }

  public Set<CubeDimension> getDimensions() {
    return cachedDims;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.CUBE;
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }

  @Override
  public void addProperties() {
    super.addProperties();
    getProperties().put(MetastoreUtil.getCubeMeasureListKey(getName()),
        StringUtils.join(measures, ","));
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        StringUtils.join(dimensions, ","));
    getProperties().put(MetastoreUtil.getParentCubeNameKey(getName()),
        parent.getName());
  }

  public static Set<String> getMeasures(String name,
      Map<String, String> props) {
    Set<String> measures = new HashSet<String>();
    String measureStr = props.get(MetastoreUtil.getCubeMeasureListKey(name));
    measures.addAll(Arrays.asList(StringUtils.split(measureStr, ',')));
    return measures;
  }

  public Set<String> getTimedDimensions() {
    String str = getProperties().get(
        MetastoreUtil.getCubeTimedDimensionListKey(getName()));
    if (str != null) {
      Set<String> timedDimensions = new HashSet<String>();
      timedDimensions.addAll(Arrays.asList(StringUtils.split(str, ',')));
      return timedDimensions;
    } else {
      return parent.getTimedDimensions();
    }
  }

  public static Set<String> getDimensions(String name,
      Map<String, String> props) {
    Set<String> dimensions = new HashSet<String>();
    String dimStr = props.get(MetastoreUtil.getCubeDimensionListKey(name));
    dimensions.addAll(Arrays.asList(StringUtils.split(dimStr, ',')));
    return dimensions;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    DerivedCube other = (DerivedCube) obj;
    if (this.getMeasures() == null) {
      if (other.getMeasures() != null) {
        return false;
      }
    } else if (!this.getMeasures().equals(other.getMeasures())) {
      return false;
    }
    if (this.getDimensions() == null) {
      if (other.getDimensions() != null) {
        return false;
      }
    } else if (!this.getDimensions().equals(other.getDimensions())) {
      return false;
    }
    return true;
  }

  public CubeDimension getDimensionByName(String dimension) {
    return parent.getDimensionByName(dimension);
  }

  public CubeMeasure getMeasureByName(String measure) {
    return parent.getMeasureByName(measure);
  }

  public CubeColumn getColumnByName(String column) {
    return parent.getColumnByName(column);
  }

  /**
   * Add a new measure
   *
   * @param measure
   * @throws HiveException
   */
  public void addMeasure(String measure) throws HiveException {
    measures.add(measure);
    getProperties().put(MetastoreUtil.getCubeMeasureListKey(getName()),
        StringUtils.join(measures, ","));
  }

  /**
   * Add a new dimension
   *
   * @param dimension
   * @throws HiveException
   */
  public void addDimension(String dimension) throws HiveException {
    dimensions.add(dimension);
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        StringUtils.join(dimensions, ","));
  }

  /**
   * Remove the dimension with name specified
   *
   * @param dimName
   */
  public void removeDimension(String dimName) {
    dimensions.remove(dimName);
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        StringUtils.join(dimensions, ","));
  }

  /**
   * Remove the measure with name specified
   *
   * @param msrName
   */
  public void removeMeasure(String msrName) {
    measures.remove(msrName);
    getProperties().put(MetastoreUtil.getCubeMeasureListKey(getName()),
        StringUtils.join(measures, ","));
  }

  @Override
  public boolean isDerivedCube() {
    return true;
  }
}
