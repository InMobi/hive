package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;

/**
 *
 * Storage is Named Interface which would represent the underlying storage of
 * the data.
 *
 */
public abstract class Storage implements Named {

  private final TableType tableType;
  private final Map<String, String> tableOrPartParameters =
      new HashMap<String, String>();
  private final List<FieldSchema> partCols = new ArrayList<FieldSchema>();
  protected final Map<String, String> serdeParameters =
      new HashMap<String, String>();
  private final String name;

  protected Storage(String name, TableType type) {
    this.tableType = type;
    this.name = name;
  }

  /**
   * Get all the partition columns of the storage.
   *
   * @return List of {@link FieldSchema}
   */
  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  /**
   * Get the table type. It can be MANAGED or EXTERNAL.
   *
   * @return TableType enum
   */
  public TableType getTableType() {
    return tableType;
  }

  /**
   * Get table properties
   *
   * @return Map<String, String>
   */
  public Map<String, String> getTableOrPartParameters() {
    return tableOrPartParameters;
  }

  /**
   * Add a partition column
   *
   * @param column having a name and type as String
   */
  public void addToPartCols(FieldSchema column) {
    partCols.add(column);
  }

  /**
   * Add more table parameters
   *
   * @param parameters
   */
  protected void addToTableOrPartParameters(Map<String, String> parameters) {
    tableOrPartParameters.putAll(parameters);
  }

  /**
   * Add a table property
   *
   * @param key property key
   * @param value property value
   */
  protected void addTableProperty(String key, String value) {
    tableOrPartParameters.put(key, value);
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

  /**
   * Set storage descriptor for the underlying hive table
   *
   * @param physicalSd {@link StorageDescriptor}
   *
   * @throws HiveException
   */
  public abstract void setSD(StorageDescriptor physicalSd) throws HiveException;

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
   * Add a partition in the underlying hive table
   *
   * @param storageTableName TableName
   * @param partSpec Partition specification
   * @param conf {@link HiveConf} object
   * @param latestInfo The latest partition info,
   *  null if latest should not be created
   *
   * @throws HiveException
   */
  public abstract void addPartition(String storageTableName,
      Map<String, String> partSpec, HiveConf conf,
      LatestInfo latestInfo)
      throws HiveException;

  /**
   * Drop the partition in the underlying hive table
   *
   * @param storageTableName TableName
   * @param partSpec Partition specification
   * @param conf {@link HiveConf} object
   *
   * @throws HiveException
   */
  public abstract void dropPartition(String storageTableName,
      List<String> partVals, HiveConf conf) throws HiveException;

  /**
   * Get the date partition key
   *
   * @return String
   */
  public static String getDatePartitionKey() {
    return StorageConstants.DATE_PARTITION_KEY;
  }

  /**
   * Get the partition spec for latest partition
   *
   * @param The partition column for latest spec
   *
   * @return latest partition spec as Map from String to String
   */
  public static Map<String, String> getLatestPartSpec(
      Map<String, String> partSpec, String partCol) {
    Map<String, String> latestSpec = new HashMap<String, String>();
    latestSpec.putAll(partSpec);
    latestSpec.put(partCol,
          StorageConstants.LATEST_PARTITION_VALUE);
   return latestSpec;
  }

  /**
   * Get the partition spec for latest partition
   *
   * @param The partition column for latest spec
   *
   * @return latest partition spec as Map from String to String
   */
  public static String getLatestPartFilter(String partCol) {
    return partCol + "='" + StorageConstants.LATEST_PARTITION_VALUE + "'";
  }

  /**
   * Get the latest partition value as List
   *
   * @return List
   */
  public static List<String> getPartitionsForLatest() {
    List<String> parts = new ArrayList<String>();
    parts.add(StorageConstants.LATEST_PARTITION_VALUE);
    return parts;
  }

  private static FieldSchema dtPart = new FieldSchema(getDatePartitionKey(),
      serdeConstants.STRING_TYPE_NAME,
      "date partition");

  /**
   * Get the date partition as fieldschema
   *
   * @return FieldSchema
   */
  public static FieldSchema getDatePartition() {
    return dtPart;
  }
}
