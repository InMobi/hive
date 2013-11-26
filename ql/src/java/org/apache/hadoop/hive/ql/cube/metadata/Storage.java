package org.apache.hadoop.hive.ql.cube.metadata;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.mortbay.log.Log;

/**
 *
 * Storage is Named Interface which would represent the underlying storage of
 * the data.
 *
 */
public abstract class Storage implements Named, PartitionMetahook {

  private final String name;

  protected Storage(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  /**
   * Get the name prefix of the storage
   *
   * @return Name followed by storage separator
   */
  public String getPrefix() {
    return getPrefix(getName());
  }

  /**
   * Get the name prefix of the storage
   *
   * @param name Name of the storage
   * @return Name followed by storage separator
   */
  public static String getPrefix(String name) {
    return name + StorageConstants.STORGAE_SEPARATOR;
  }

  public static final class LatestInfo {
    Map<String, LatestPartColumnInfo> latestParts =
        new HashMap<String, LatestPartColumnInfo>();
    void addLatestPartInfo(String partCol, LatestPartColumnInfo partInfo) {
      latestParts.put(partCol, partInfo);
    }
  }

  public static final class LatestPartColumnInfo {
    final Map<String, String> partParams;
    public LatestPartColumnInfo(Map<String, String> partParams) {
      this.partParams = partParams;
    }
  }

  /**
   * Get the storage table descriptor for the given parent table.
   *
   * @param client The metastore client
   * @param parent Is either Fact or Dimension table
   * @param crtTbl Create table info
   * @return Table describing the storage table
   *
   * @throws HiveException
   */
  public Table getStorageTable(Hive client,
      Table parent, StorageTableDesc crtTbl) throws HiveException {
    String storageTableName = MetastoreUtil.getStorageTableName(
        parent.getTableName(), this.getPrefix());
    Table tbl = client.newTable(storageTableName);
    tbl.getTTable().setSd(new StorageDescriptor(parent.getTTable().getSd()));

    if (crtTbl.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtTbl.getTblProps());
    }

    if (crtTbl.getPartCols() != null) {
      tbl.setPartCols(crtTbl.getPartCols());
    }
    if (crtTbl.getNumBuckets() != -1) {
      tbl.setNumBuckets(crtTbl.getNumBuckets());
    }

    if (crtTbl.getStorageHandler() != null) {
      tbl.setProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE,
          crtTbl.getStorageHandler());
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();

    if (crtTbl.getSerName() == null) {
      if (storageHandler == null) {
        tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      } else {
        String serDeClassName = storageHandler.getSerDeClass().getName();
        tbl.setSerializationLib(serDeClassName);
      }
    } else {
      // let's validate that the serde exists
      tbl.setSerializationLib(crtTbl.getSerName());
    }

    if (crtTbl.getFieldDelim() != null) {
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, crtTbl.getFieldDelim());
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, crtTbl.getFieldDelim());
    }
    if (crtTbl.getFieldEscape() != null) {
      tbl.setSerdeParam(serdeConstants.ESCAPE_CHAR, crtTbl.getFieldEscape());
    }

    if (crtTbl.getCollItemDelim() != null) {
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, crtTbl.getCollItemDelim());
    }
    if (crtTbl.getMapKeyDelim() != null) {
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
    }
    if (crtTbl.getLineDelim() != null) {
      tbl.setSerdeParam(serdeConstants.LINE_DELIM, crtTbl.getLineDelim());
    }

    if (crtTbl.getSerdeProps() != null) {
      Iterator<Entry<String, String>> iter = crtTbl.getSerdeProps().entrySet()
          .iterator();
      while (iter.hasNext()) {
        Entry<String, String> m = iter.next();
        tbl.setSerdeParam(m.getKey(), m.getValue());
      }
    }

    if (crtTbl.getBucketCols() != null) {
      tbl.setBucketCols(crtTbl.getBucketCols());
    }
    if (crtTbl.getSortCols() != null) {
      tbl.setSortCols(crtTbl.getSortCols());
    }
    if (crtTbl.getComment() != null) {
      tbl.setProperty("comment", crtTbl.getComment());
    }
    if (crtTbl.getLocation() != null) {
      tbl.setDataLocation(new Path(crtTbl.getLocation()).toUri());
    }

    if (crtTbl.getSkewedColNames() != null) {
      tbl.setSkewedColNames(crtTbl.getSkewedColNames());
    }
    if (crtTbl.getSkewedColValues() != null) {
      tbl.setSkewedColValues(crtTbl.getSkewedColValues());
    }

    tbl.setStoredAsSubDirectories(crtTbl.isStoredAsSubDirectories());

    tbl.setInputFormatClass(crtTbl.getInputFormat());
    tbl.setOutputFormatClass(crtTbl.getOutputFormat());

    tbl.getTTable().getSd().setInputFormat(
        tbl.getInputFormatClass().getName());
    tbl.getTTable().getSd().setOutputFormat(
        tbl.getOutputFormatClass().getName());

    if (crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
    }
    return tbl;
  }

  /**
   * Add a partition in the underlying hive table and
   *  update latest partition links
   *
   * @param client The metastore client
   * @param addPartitionDesc add Partition specification
   * @param latestInfo The latest partition info,
   *  null if latest should not be created
   *
   * @throws HiveException
   */
  public void addPartition(Hive client,
      StoragePartitionDesc addPartitionDesc,
      LatestInfo latestInfo)
          throws HiveException {
    preAddPartition(addPartitionDesc);
    boolean success = false;
    try {
      String tableName = MetastoreUtil.getStorageTableName(
          addPartitionDesc.getCubeTableName(), this.getPrefix());
      String dbName = addPartitionDesc.getDbName();
      if (dbName == null) {
        dbName = SessionState.get().getCurrentDatabase();
      }
      Table storageTbl = client.getTable(dbName,
          tableName);
      Path location = null;
      if (addPartitionDesc.getLocation() != null) {
        Path partLocation = new Path(addPartitionDesc.getLocation());
        if (partLocation.isAbsolute()) {
          location = partLocation;
        } else {
          location = new Path(storageTbl.getPath(), partLocation);
        }
      }
      Log.info("Adding partition with partSpec:" + addPartitionDesc.getStoragePartSpec());
      client.createPartition(storageTbl, addPartitionDesc.getStoragePartSpec(),
          location, addPartitionDesc.getPartParams(),
          addPartitionDesc.getInputFormat(), addPartitionDesc.getOutputFormat(),
          addPartitionDesc.getNumBuckets(), addPartitionDesc.getCols(),
          addPartitionDesc.getSerializationLib(),
          addPartitionDesc.getSerdeParams(),
          addPartitionDesc.getBucketCols(),
          addPartitionDesc.getSortCols());

      if (latestInfo != null) {
        for (Map.Entry<String, LatestPartColumnInfo> entry :
          latestInfo.latestParts.entrySet()) {
          // symlink this partition to latest
          List<Partition> latest;
          String latestPartCol = entry.getKey();
          try {
            latest = client.getPartitionsByFilter(storageTbl,
                StorageConstants.getLatestPartFilter(latestPartCol));
          } catch (Exception e) {
            throw new HiveException("Could not get latest partition", e);
          }
          if (!latest.isEmpty()) {
            client.dropPartition(storageTbl.getTableName(),
                latest.get(0).getValues(), false);
          }
          Map<String, String> partParams = addPartitionDesc.getPartParams();
          if (partParams == null) {
            partParams = new HashMap<String, String>();
          }
          partParams.putAll(entry.getValue().partParams);
          client.createPartition(storageTbl, StorageConstants.getLatestPartSpec(
              addPartitionDesc.getStoragePartSpec(),
              latestPartCol),
              location, partParams, addPartitionDesc.getInputFormat(),
              addPartitionDesc.getOutputFormat(),
              addPartitionDesc.getNumBuckets(), addPartitionDesc.getCols(),
              addPartitionDesc.getSerializationLib(),
              addPartitionDesc.getSerdeParams(),
              addPartitionDesc.getBucketCols(),
              addPartitionDesc.getSortCols());
        }
      }
      commitAddPartition(addPartitionDesc);
      success = true;
    } finally {
      if (!success) {
        rollbackAddPartition(addPartitionDesc);
      }
    }
  }


  /**
   * Drop the partition in the underlying hive table and
   *  update latest partition link
   *
   * @param client The metastore client
   * @param storageTableName TableName
   * @param partSpec Partition specification
   * @param latestInfo The latest partition info if it needs update,
   *  null if latest should not be updated
   *
   * @throws HiveException
   */
  public void dropPartition(Hive client, String storageTableName,
      List<String> partVals, LatestInfo updateLatestInfo) throws HiveException {
    preDropPartition(storageTableName, partVals);
    boolean success = false;
    try {
      client.dropPartition(storageTableName, partVals, false);
      // TODO update latest info
      commitDropPartition(storageTableName, partVals);
    } finally {
      if (!success) {
        rollbackDropPartition(storageTableName, partVals);
      }
    }
  }

  public static Storage createInstance(String storageClassName, String storageName)
      throws HiveException {
    try {
      Class<?> clazz = Class.forName(storageClassName);
      Constructor<?> constructor = clazz.getConstructor(String.class);
      Storage storage = (Storage) constructor.newInstance(new Object[]
          {storageName});
      return storage;
    } catch (Exception e) {
      throw new HiveException("Could not create storage class" + storageClassName, e);
    }

  }
}
