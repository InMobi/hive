package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;

public interface PartitionMetahook {

  /**
   * Called before calling add partition
   *
   * @param addPartitionDesc
   * @throws HiveException
   */
  public void preAddPartition(AddPartitionDesc addPartitionDesc) throws HiveException;

  /**
   * Called after successfully adding the partition
   *
   * @param addPartitionDesc
   * @throws HiveException
   */
  public void commitAddPartition(AddPartitionDesc addPartitionDesc) throws HiveException;

  /**
   * Called if add partition fails.
   *
   * @param addPartitionDesc
   * @throws HiveException
   */
  public void rollbackAddPartition(AddPartitionDesc addPartitionDesc) throws HiveException;

  /**
   * Called before calling drop partition
   *
   * @param addPartitionDesc
   * @throws HiveException
   */
  public void preDropPartition(String storageTableName,
      List<String> partVals) throws HiveException;

  /**
   * Called after successfully adding the partition
   *
   * @param addPartitionDesc
   * @throws HiveException
   */
  public void commitDropPartition(String storageTableName,
      List<String> partVals) throws HiveException;

  /**
   * Called if add partition fails.
   *
   * @param addPartitionDesc
   * @throws HiveException
   */
  public void rollbackDropPartition(String storageTableName,
      List<String> partVals) throws HiveException;
}
