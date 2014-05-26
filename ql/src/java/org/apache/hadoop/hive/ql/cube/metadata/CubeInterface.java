package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Set;

public interface CubeInterface extends Named {

  public Set<CubeMeasure> getMeasures();

  public Set<CubeDimension> getDimensions();

  public CubeDimension getDimensionByName(String dimension);

  public CubeMeasure getMeasureByName(String measure);

  public CubeColumn getColumnByName(String column);

  public Set<String> getTimedDimensions();
  
  public boolean isDerivedCube();
  
  public Set<String> getMeasureNames();

  public Set<String> getDimensionNames();
}
