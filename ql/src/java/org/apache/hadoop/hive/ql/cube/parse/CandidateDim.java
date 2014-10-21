package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.session.SessionState;

class CandidateDim implements CandidateTable {
  final CubeDimensionTable dimtable;
  String storageTable;
  String whereClause;
  private boolean dbResolved = false;
  private boolean whereClauseAdded = false;
  private Dimension baseTable;

  public boolean isWhereClauseAdded() {
    return whereClauseAdded;
  }

  public void setWhereClauseAdded() {
    this.whereClauseAdded = true;
  }

  CandidateDim(CubeDimensionTable dimtable, Dimension dim) {
    this.dimtable = dimtable;
    this.baseTable = dim;
  }

  @Override
  public String toString() {
    return dimtable.toString();
  }

  public String getStorageString(String alias) {
    if (!dbResolved) {
      String database = SessionState.get().getCurrentDatabase();
      // Add database name prefix for non default database
      if (StringUtils.isNotBlank(database) && !"default".equalsIgnoreCase(database)) {
        storageTable = database + "." + storageTable;
      }
      dbResolved = true;
    }
    return storageTable + " " + alias;
  }

  @Override
  public Dimension getBaseTable() {
    return baseTable;
  }

  @Override
  public CubeDimensionTable getTable() {
    return dimtable;
  }

  @Override
  public String getName() {
    return dimtable.getName();
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CandidateDim other = (CandidateDim) obj;

    if (this.getTable() == null) {
      if (other.getTable() != null) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getTable() == null) ? 0 :
      getTable().getName().toLowerCase().hashCode());
    return result;
  }

  @Override
  public Collection<String> getColumns() {
    return dimtable.getAllFieldNames();
  }
}