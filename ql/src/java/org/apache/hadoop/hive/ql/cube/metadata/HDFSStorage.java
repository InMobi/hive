package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class HDFSStorage extends Storage {

  public HDFSStorage(String name) {
    super(name, null);
  }

  public HDFSStorage(Table hiveTable) {
    super(hiveTable);
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
