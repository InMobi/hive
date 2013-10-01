package org.apache.hadoop.hive.ql.cube.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public final class Cube extends AbstractCubeTable {
  private final Set<CubeMeasure> measures;
  private final Set<CubeDimension> dimensions;
  private static final List<FieldSchema> columns = new ArrayList<FieldSchema>();
  private final Map<String, CubeMeasure> measureMap;
  private final Map<String, CubeDimension> dimMap;

  static {
    columns.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public Cube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions) {
    this(name, measures, dimensions, new HashMap<String, String>());
  }

  public Cube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions, Map<String, String> properties) {
    this(name, measures, dimensions, properties, 0L);
  }

  public Cube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions, Map<String, String> properties,
      double weight) {
    super(name, columns, properties, weight);
    this.measures = measures;
    this.dimensions = dimensions;

    measureMap = new HashMap<String, CubeMeasure>();
    for (CubeMeasure m : measures) {
      measureMap.put(m.getName().toLowerCase(), m);
    }

    dimMap = new HashMap<String, CubeDimension>();
    for (CubeDimension dim : dimensions) {
      dimMap.put(dim.getName().toLowerCase(), dim);
    }

    addProperties();
  }
  public Cube(Table tbl) {
    super(tbl);
    this.measures = getMeasures(getName(), getProperties());
    this.dimensions = getDimensions(getName(), getProperties());
    measureMap = new HashMap<String, CubeMeasure>();
    for (CubeMeasure m : measures) {
      measureMap.put(m.getName().toLowerCase(), m);
    }

    dimMap = new HashMap<String, CubeDimension>();
    for (CubeDimension dim : dimensions) {
      addAllDimsToMap(dim);
    }
  }

  private void addAllDimsToMap(CubeDimension dim) {
    dimMap.put(dim.getName().toLowerCase(), dim);
    if (dim instanceof HierarchicalDimension) {
      for (CubeDimension d : ((HierarchicalDimension)dim).getHierarchy()) {
        addAllDimsToMap(d);
      }
    }
  }
  public Set<CubeMeasure> getMeasures() {
    return measures;
  }

  public Set<CubeDimension> getDimensions() {
    return dimensions;
  }

  public Set<String> getTimedDimensions() {
    String str = getProperties().get(
        MetastoreUtil.getCubeTimedDimensionListKey(getName()));
    if (str != null) {
      Set<String> timedDimensions = new HashSet<String>();
      timedDimensions.addAll(Arrays.asList(StringUtils.split(str, ',')));
      return timedDimensions;
    }
    return null;
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
        MetastoreUtil.getNamedStr(measures));
    setMeasureProperties(getProperties(), measures);
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        MetastoreUtil.getNamedStr(dimensions));
    setDimensionProperties(getProperties(), dimensions);
  }

  private static void setMeasureProperties(Map<String, String> props,
                                           Set<CubeMeasure> measures) {
    for (CubeMeasure measure : measures) {
      measure.addProperties(props);
    }
  }

  private static void setDimensionProperties(Map<String, String> props,
                                             Set<CubeDimension> dimensions) {
    for (CubeDimension dimension : dimensions) {
      dimension.addProperties(props);
    }
  }

  public static Set<CubeMeasure> getMeasures(String name,
      Map<String, String> props) {
    Set<CubeMeasure> measures = new HashSet<CubeMeasure>();
    String measureStr = props.get(MetastoreUtil.getCubeMeasureListKey(name));
    String[] names = measureStr.split(",");
    for (String measureName : names) {
      String className = props.get(MetastoreUtil.getMeasureClassPropertyKey(
          measureName));
      CubeMeasure measure;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        measure = (CubeMeasure) constructor.newInstance(new Object[]
            {measureName, props});
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Invalid measure", e);
      }
      measures.add(measure);
    }
    return measures;
  }

  public static Set<CubeDimension> getDimensions(String name,
      Map<String, String> props) {
    Set<CubeDimension> dimensions = new HashSet<CubeDimension>();
    String dimStr = props.get(MetastoreUtil.getCubeDimensionListKey(name));
    String[] names = dimStr.split(",");
    for (String dimName : names) {
      String className = props.get(MetastoreUtil.getDimensionClassPropertyKey(
          dimName));
      CubeDimension dim;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        dim = (CubeDimension) constructor.newInstance(new Object[]
            {dimName, props});
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      }
      dimensions.add(dim);
    }
    return dimensions;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    Cube other = (Cube) obj;
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
    return dimMap.get(dimension == null ? dimension : dimension.toLowerCase());
  }

  public CubeMeasure getMeasureByName(String measure) {
    return measureMap.get(measure == null ? measure : measure.toLowerCase());
  }

  public CubeColumn getColumnByName(String column) {
    CubeColumn cubeCol = (CubeColumn)getMeasureByName(column);
    if (cubeCol == null) {
      cubeCol = (CubeColumn)getDimensionByName(column);
    }
    return cubeCol;
  }

  public void addMeasure(CubeMeasure measure) throws HiveException {
    if (measure == null) {
      throw new NullPointerException("Cannot add null measure");
    }
    if (measures.contains(measure))  {
      throw new HiveException("Measure " + measure + " is already present in cube " + getName());
    }

    measures.add(measure);
    measureMap.put(measure.getName(), measure);
  }

  public void addDimension(CubeDimension dimension) throws HiveException {
    if (dimension == null) {
      throw new NullPointerException("Cannot add null dimension");
    }

    if (dimensions.contains(dimension)) {
      throw new HiveException("Dimension " + dimension + " is already present in cube " + getName());
    }

    dimensions.add(dimension);
    dimMap.put(dimension.getName(), dimension);
  }

  public void addTimedDimension(String timedDimension) throws HiveException {
    if (timedDimension == null || timedDimension.isEmpty()) {
      throw new HiveException("Invalid timed dimension " + timedDimension);
    }

    Set<String> timeDims = getTimedDimensions();
    if (timeDims.contains(timedDimension)) {
      throw new HiveException("Timed dimension " + timedDimension + " is already present in cube "+ getName());
    }

    timeDims.add(timedDimension);
    getProperties().put(MetastoreUtil.getCubeTimedDimensionListKey(getName()), StringUtils.join(timeDims, ","));
  }
}
