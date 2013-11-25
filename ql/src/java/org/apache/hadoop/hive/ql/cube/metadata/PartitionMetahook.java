package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public interface PartitionMetahook {

  /**
   * Called before calling add partition
   *
   * @param StoragePartitionDesc
   * @throws HiveException
   */
  public void preAddPartition(StoragePartitionDesc StoragePartitionDesc) throws HiveException;

  /**
   * Called after successfully adding the partition
   *
   * @param StoragePartitionDesc
   * @throws HiveException
   */
  public void commitAddPartition(StoragePartitionDesc StoragePartitionDesc) throws HiveException;

  /**
   * Called if add partition fails.
   *
   * @param StoragePartitionDesc
   * @throws HiveException
   */
  public void rollbackAddPartition(StoragePartitionDesc StoragePartitionDesc) throws HiveException;

  /**
   * Called before calling drop partition
   *
   * @param storageTableName
   * @param partVals
   * @throws HiveException
   */
  public void preDropPartition(String storageTableName,
      List<String> partVals) throws HiveException;

  /**
   * Called after successfully droping the partition
   *
   * @param storageTableName
   * @param partVals
   * @throws HiveException
   */
  public void commitDropPartition(String storageTableName,
      List<String> partVals) throws HiveException;

  /**
   * Called if drop partition fails.
   *
   * @param storageTableName
   * @param partVals
   */
  public void rollbackDropPartition(String storageTableName,
      List<String> partVals) throws HiveException;
}
