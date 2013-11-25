package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;

public class StorageTableDesc extends CreateTableDesc {
  private static final long serialVersionUID = 1L;

  private List<String> timePartCols;

  /**
   * @return the timePartCols
   */
  public List<String> getTimePartCols() {
    return timePartCols;
  }

  public void setTimePartCols(List<String> timePartCols) {
    this.timePartCols = timePartCols;
    if (super.getTblProps() == null) {
      super.setTblProps(new HashMap<String, String>());
    }
    super.getTblProps().put(MetastoreConstants.TIME_PART_COLUMNS,
        StringUtils.join(this.timePartCols, ','));
  }

  /**
   * @deprecated
   */
  @Override
  @Deprecated
  public String getTableName() {
    return super.getTableName();
  }

  /**
   * This is not honored.
   *
   * @deprecated
   */
  @Override
  @Deprecated
  public void setTableName(String tableName) {
  }

}
