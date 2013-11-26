package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;

public class StoragePartitionDesc extends AddPartitionDesc {

  private static final long serialVersionUID = 1L;

  private Map<String, Date> timePartSpec;
  private Map<String, String> nonTimePartSpec;
  private UpdatePeriod updatePeriod;
  private Map<String, String> fullPartSpec;
  private String cubeTableName;

  public StoragePartitionDesc() {
  }

  public StoragePartitionDesc(String cubeTableName,
      Map<String, Date> timePartSpec, Map<String, String> nonTimePartSpec,
      UpdatePeriod updatePeriod) {
    this.cubeTableName = cubeTableName;
    this.timePartSpec = timePartSpec;
    this.nonTimePartSpec = nonTimePartSpec;
    this.updatePeriod = updatePeriod;
  }

  /**
   * @return the table we're going to add the partitions to.
   * @deprecated
   */
  @Override
  @Deprecated
  public String getTableName() {
    return super.getTableName();
  }

  /**
   * @param tableName
   *          the table we're going to add the partitions to.
   * @deprecated
   */
  @Override
  @Deprecated
  public void setTableName(String tableName) {
    super.setTableName(tableName);
  }

  /**
   * @return the cubeTableName
   */
  public String getCubeTableName() {
    return cubeTableName;
  }

  /**
   * @param cubeTableName the cubeTableName to set
   */
  public void setCubeTableName(String cubeTableName) {
    this.cubeTableName = cubeTableName;
  }

  /**
   * @param partSpec
   *          partition specification
   * @deprecated Use setTimePartSpec and setNonTimePartSpec
   */
  @Override
  @Deprecated
  public void setPartSpec(LinkedHashMap<String, String> partSpec) {
  }

  /**
  * @deprecated Use getStoragePartSpec
  */
  @Override
  @Deprecated
  public LinkedHashMap<String, String> getPartSpec() {
    return super.getPartSpec();
  }

  public Map<String, String> getStoragePartSpec() {
    if (fullPartSpec == null) {
      fullPartSpec = new HashMap<String, String>();
      for (Map.Entry<String, Date> entry : timePartSpec.entrySet()) {
        fullPartSpec.put(entry.getKey(), updatePeriod.format().format(entry.getValue()));
      }
      if (nonTimePartSpec != null) {
        fullPartSpec.putAll(nonTimePartSpec);
      }
    }
    return fullPartSpec;
  }

  /**
   * @return the timePartSpec
   */
  public Map<String, Date> getTimePartSpec() {
    return timePartSpec;
  }

  /**
   * @param timePartSpec the timePartSpec to set
   */
  public void setTimePartSpec(Map<String, Date> timePartSpec) {
    this.timePartSpec = timePartSpec;
  }

  /**
   * @return the nonTimePartSpec
   */
  public Map<String, String> getNonTimePartSpec() {
    return nonTimePartSpec;
  }

  /**
   * @param nonTimePartSpec the nonTimePartSpec to set
   */
  public void setNonTimePartSpec(Map<String, String> nonTimePartSpec) {
    this.nonTimePartSpec = nonTimePartSpec;
  }

  /**
   * @return the updatePeriod
   */
  public UpdatePeriod getUpdatePeriod() {
    return updatePeriod;
  }

  /**
   * @param updatePeriod the updatePeriod to set
   */
  public void setUpdatePeriod(UpdatePeriod updatePeriod) {
    this.updatePeriod = updatePeriod;
  }

}
