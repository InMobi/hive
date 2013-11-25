package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public class HDFSStorage extends Storage {

  public HDFSStorage(String name) {
    super(name);
  }

  @Override
  public void preAddPartition(StoragePartitionDesc addPartitionDesc) throws HiveException {
    // No op

  }

  @Override
  public void commitAddPartition(StoragePartitionDesc addPartitionDesc) throws HiveException {
    // No op

  }

  @Override
  public void rollbackAddPartition(StoragePartitionDesc addPartitionDesc) throws HiveException {
    // No op

  }

  @Override
  public void preDropPartition(String storageTableName, List<String> partVals) throws HiveException {
    // No op

  }

  @Override
  public void commitDropPartition(String storageTableName, List<String> partVals)
      throws HiveException {
    // No op

  }

  @Override
  public void rollbackDropPartition(String storageTableName, List<String> partVals)
      throws HiveException {
    // No op

  }
}
