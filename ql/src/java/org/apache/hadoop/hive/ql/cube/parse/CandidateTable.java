package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collection;

import org.apache.hadoop.hive.ql.cube.metadata.AbstractCubeTable;

/**
 * Candidate table interface
 *
 */
interface CandidateTable {

  /**
   * Get storage string of the base table alias passed
   *
   * @param alias
   *
   * @return storage string
   */
  public String getStorageString(String alias);

  /**
   * Get candidate table
   *
   * @return Candidate fact or dim table
   */
  public AbstractCubeTable getTable();

  /**
   * Get base table of the candidate table
   *
   * @return Cube or DerivedCube or Dimesions
   */
  public AbstractCubeTable getBaseTable();

  /**
   * Get name of the candidate table
   *
   * @return name
   */
  public String getName();

  /**
   * Get columns of candidate table
   *
   * @return set or list of columns
   */
  public Collection<String> getColumns();
}