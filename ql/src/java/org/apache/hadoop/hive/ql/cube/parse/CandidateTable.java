package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collection;

import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;

interface CandidateTable {
  public String getStorageString(String alias);
  public AbstractCubeTable getTable();
  public AbstractCubeTable getBaseTable();
  public String getName();
  public Collection<String> getColumns();
}