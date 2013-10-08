package org.apache.hadoop.hive.ql.cube.metadata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;

/**
 * Wrapper class around Hive metastore to do cube metastore operations.
 *
 */
public class CubeMetastoreClient {
  private Hive metastore;
  private final HiveConf config;
  private boolean enableCaching;

  private CubeMetastoreClient(HiveConf conf) {
    this.config = conf;
  }

  private final List<String> allHiveTableNames = new ArrayList<String>();
  // map from table name to Table
  private final Map<String, Table> allHiveTables = new HashMap<String, Table>();
  // map from cube name to Cube
  private final Map<String, Cube> allCubes = new HashMap<String, Cube>();
  // map from dim name to CubeDimensionTable
  private final Map<String, CubeDimensionTable> allDims = new HashMap<String, CubeDimensionTable>();
  // map from cube to fact tables
  private final Map<Cube, List<CubeFactTable>> allFactTables = new HashMap<Cube, List<CubeFactTable>>();

  private static CubeMetastoreClient instance;

  private SchemaGraph schemaGraph;
  /**
   * Get the instance of {@link CubeMetastoreClient} corresponding
   * to {@link HiveConf}
   *
   * @param conf
   * @return CubeMetastoreClient
   * @throws HiveException
   */
  public static CubeMetastoreClient getInstance(HiveConf conf)
      throws HiveException {
    if (instance == null) {
      instance = new CubeMetastoreClient(conf);
    } else if (conf.getBoolean(MetastoreConstants.METASTORE_NEEDS_REFRESH, false)) {
      // refresh the client
      instance = new CubeMetastoreClient(conf);
    }
    instance.metastore = Hive.get(conf);
    instance.enableCaching = conf.getBoolean(MetastoreConstants.METASTORE_ENABLE_CACHING, true);
    return instance;
  }

  private Hive getClient() {
    return metastore;
  }

  /**
   * Close the current metastore client
   */
  public static void close() {
    Hive.closeCurrent();
  }

  public void setCurrentDatabase(String currentDatabase) {
    SessionState.get().setCurrentDatabase(currentDatabase);
  }

  public String getCurrentDatabase() {
    return SessionState.get().getCurrentDatabase();
  }

  private StorageDescriptor createStorageHiveTable(String tableName,
      StorageDescriptor sd,
      Map<String, String> parameters, TableType type,
      List<FieldSchema> partCols) throws HiveException {
    try {
      Table tbl = getClient().newTable(tableName.toLowerCase());
      tbl.getTTable().getParameters().putAll(parameters);
      tbl.getTTable().setSd(sd);
      if (partCols != null && partCols.size() != 0) {
        tbl.setPartCols(partCols);
      }
      tbl.setTableType(type);
      getClient().createTable(tbl);
      return tbl.getTTable().getSd();
    } catch (Exception e) {
      throw new HiveException("Exception creating table", e);
    }
  }

  private StorageDescriptor createCubeHiveTable(AbstractCubeTable table)
      throws HiveException {
    try {
      Table tbl = getClient().newTable(table.getName().toLowerCase());
      tbl.setTableType(TableType.MANAGED_TABLE);
      tbl.getTTable().getSd().setCols(table.getColumns());
      tbl.getTTable().getParameters().putAll(table.getProperties());
      getClient().createTable(tbl);
      return tbl.getTTable().getSd();
    } catch (Exception e) {
      throw new HiveException("Exception creating table", e);
    }
  }

  private void createFactStorage(String factName, Storage storage,
      StorageDescriptor parentSD)
          throws HiveException {
    String storageTblName = MetastoreUtil.getFactStorageTableName(factName,
        storage.getPrefix());
    createStorage(storageTblName, storage, parentSD);
  }

  private void createDimStorage(String dimName, Storage storage,
      StorageDescriptor parentSD)
          throws HiveException {
    String storageTblName = MetastoreUtil.getDimStorageTableName(dimName,
        storage.getPrefix());
    createStorage(storageTblName, storage, parentSD);
  }

  private StorageDescriptor getStorageSD(Storage storage,
      StorageDescriptor parentSD) throws HiveException {
    StorageDescriptor physicalSd = new StorageDescriptor(parentSD);
    storage.setSD(physicalSd);
    return physicalSd;
  }

  private StorageDescriptor getCubeTableSd(AbstractCubeTable table)
      throws HiveException {
    Table cubeTbl = getTable(table.getName());
    return cubeTbl.getTTable().getSd();
  }

  private void createStorage(String name,
      Storage storage, StorageDescriptor parentSD) throws HiveException {
    StorageDescriptor physicalSd = getStorageSD(storage, parentSD);
    createStorageHiveTable(name,
      physicalSd, storage.getTableParameters(),
      storage.getTableType(), storage.getPartCols());
  }

  private Map<String, Set<UpdatePeriod>> getUpdatePeriods(
      Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods) {
    if (storageAggregatePeriods != null) {
      Map<String, Set<UpdatePeriod>> updatePeriods =
          new HashMap<String, Set<UpdatePeriod>>();
      for (Map.Entry<Storage, Set<UpdatePeriod>> entry :
          storageAggregatePeriods.entrySet()) {
        updatePeriods.put(entry.getKey().getName(), entry.getValue());
      }
      return updatePeriods;
    } else {
      return null;
    }
  }

  /**
   * Create cube in metastore defined by {@link Cube} object
   *
   * @param cube the {@link Cube} object.
   * @throws HiveException
   */
  public void createCube(Cube cube) throws HiveException {
    createCubeHiveTable(cube);
  }

  /**
   * Create cube defined by measures and dimensions
   *
   * @param name Name of the cube
   * @param measures Measures of the cube
   * @param dimensions Dimensions of the cube
   *
   * @throws HiveException
   */
  public void createCube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions) throws HiveException {
    Cube cube = new Cube(name, measures, dimensions);
    createCube(cube);
  }

  /**
   * Create cube defined by measures, dimensions and properties
   *
   * @param name Name of the cube
   * @param measures Measures of the cube
   * @param dimensions Dimensions of the cube
   * @param properties Properties of the cube
   * @throws HiveException
   */
  public void createCube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions, Map<String, String> properties)
          throws HiveException {
    Cube cube = new Cube(name, measures, dimensions, properties);
    createCube(cube);
  }

  /**
   * Create a cube fact table
   *
   * @param cubeName The cube name to which fact belongs to.
   * @param factName The fact name
   * @param columns The columns of fact table
   * @param storageAggregatePeriods Aggregate periods for the storages
   * @param weight Weight of the cube
   * @param properties Properties of fact table
   *
   * @throws HiveException
   */
  public void createCubeFactTable(String cubeName, String factName,
      List<FieldSchema> columns,
      Map<Storage, Set<UpdatePeriod>> storageAggregatePeriods, double weight,
      Map<String, String> properties)
          throws HiveException {
    CubeFactTable factTable = new CubeFactTable(cubeName, factName, columns,
        getUpdatePeriods(storageAggregatePeriods), weight, properties);
    createCubeTable(factTable, storageAggregatePeriods.keySet());
  }

  /**
   * Create a cube dimension table
   *
   * @param dimName dimensions name
   * @param columns Columns of the dimension table
   * @param weight Weight of the dimension table
   * @param dimensionReferences References to other dimensions
   * @param storages Storages on which dimension is available
   * @param properties Properties of dimension table
   *
   * @throws HiveException
   */
  public void createCubeDimensionTable(String dimName,
      List<FieldSchema> columns, double weight,
      Map<String, List<TableReference>> dimensionReferences,
      Set<Storage> storages, Map<String, String> properties)
          throws HiveException {
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, columns,
        weight, getStorageNames(storages), dimensionReferences, properties);
    createCubeTable(dimTable, storages);
  }

  private Set<String> getStorageNames(Set<Storage> storages) {
    Set<String> storageNames = new HashSet<String>();
    for (Storage storage : storages) {
      storageNames.add(storage.getName());
    }
    return storageNames;
  }

  private Map<String, UpdatePeriod> getDumpPeriods(
      Map<Storage, UpdatePeriod> storageDumpPeriods) {
    if (storageDumpPeriods != null) {
      Map<String, UpdatePeriod> updatePeriods =
          new HashMap<String, UpdatePeriod>();
      for (Map.Entry<Storage, UpdatePeriod> entry : storageDumpPeriods
          .entrySet()) {
        updatePeriods.put(entry.getKey().getName(), entry.getValue());
      }
      return updatePeriods;
    } else {
      return null;
    }
  }

  /**
   * Create a cube dimension table
   *
   * @param dimName dimensions name
   * @param columns Columns of the dimension table
   * @param weight Weight of the dimension table
   * @param dimensionReferences References to other dimensions
   * @param dumpPeriods Storages and their dump periods on which dimension
   *  is available
   * @param properties properties of dimension table
   * @throws HiveException
   */
  public void createCubeDimensionTable(String dimName,
      List<FieldSchema> columns, double weight,
      Map<String, List<TableReference>> dimensionReferences,
      Map<Storage, UpdatePeriod> dumpPeriods,
      Map<String, String> properties)
          throws HiveException {
    // add date partitions for storages with dumpPeriods
    addDatePartitions(dumpPeriods);
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, columns,
        weight, getDumpPeriods(dumpPeriods), dimensionReferences, properties);
    createCubeTable(dimTable, dumpPeriods.keySet());
  }

  private void addDatePartitions(Map<Storage, UpdatePeriod> dumpPeriods) {
    for (Map.Entry<Storage, UpdatePeriod> entry : dumpPeriods.entrySet()) {
      if (entry.getValue() != null) {
        entry.getKey().addToPartCols(Storage.getDatePartition());
      }
    }
  }

  /**
   * Create cube fact table defined by {@link CubeFactTable} object
   *
   * @param factTable The {@link CubeFactTable} object
   * @param storageAggregatePeriods Storages and their aggregate periods on
   *  which fact is available
   * @throws HiveException
   */
  public void createCubeTable(CubeFactTable factTable,
      Set<Storage> storages)
          throws HiveException {
    // create virtual cube table in metastore
    StorageDescriptor sd = createCubeHiveTable(factTable);

    if (storages != null) {
      // create tables for each storage
      for (Storage storage : storages) {
        createFactStorage(factTable.getName(), storage, sd);
      }
    }
  }

  /**
   * Create cube dimension table defined by {@link CubeDimensionTable} object
   *
   * @param dimTable The {@link CubeDimensionTable} object
   * @param storages Storages on which dimension is available
   * @throws HiveException
   */
  public void createCubeTable(CubeDimensionTable dimTable,
      Set<Storage> storages) throws HiveException {
    // create virtual cube table in metastore
    StorageDescriptor sd = createCubeHiveTable(dimTable);

    if (storages != null) {
      // create tables for each storage
      for (Storage storage : storages) {
        createDimStorage(dimTable.getName(), storage, sd);
      }
    }
  }

  /**
   * Add storage to fact
   *
   * @param table The CubeFactTable
   * @param storage The storage
   * @param updatePeriods Update periods of the fact on the storage
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void addStorage(CubeFactTable table, Storage storage,
      Set<UpdatePeriod> updatePeriods) throws HiveException {
    table.addStorage(storage.getName(), updatePeriods);
    createFactStorage(table.getName(), storage, getCubeTableSd(table));
    alterCubeTable(table.getName(), getTable(table.getName()), table);
    updateFactCache(table.getName());
  }

  /**
   * Add storage to dimension
   *
   * @param table The CubeDimensionTable
   * @param storage The storage
   * @param dumpPeriod The dumpPeriod if any, null otherwise
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void addStorage(CubeDimensionTable table, Storage storage,
      UpdatePeriod dumpPeriod) throws HiveException {
    table.alterSnapshotDumpPeriod(storage.getName(), dumpPeriod);
    createDimStorage(table.getName(), storage, getCubeTableSd(table));
    alterCubeTable(table.getName(), getTable(table.getName()), table);
    updateDimCache(table.getName());
  }

  static List<String> getPartitionValues(Table tbl,
      Map<String, String> partSpec) throws HiveException {
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartitionKeys()) {
      String val = partSpec.get(field.getName());
      if (val == null) {
        throw new HiveException("partition spec is invalid. field.getName()" +
            " does not exist in input.");
      }
      pvals.add(val);
    }
    return pvals;
  }

  /**
   * Add time partition to the fact on given storage for an updateperiod
   *
   * @param table The {@link CubeFactTable} object
   * @param storage The {@link Storage} object
   * @param updatePeriod The updatePeriod
   * @param partitionTimestamp partition timestamp
   * @param latestPartCol The partitoin column name if latest symlink has to be
   *        created, null otherwise
   * @throws HiveException
   */
  public void addPartition(CubeFactTable table, Storage storage,
      UpdatePeriod updatePeriod, Map<String, Date> partitionTimestamps,
      String latestPartCol)
          throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        table.getName(), storage.getPrefix());
    addPartition(storageTableName, storage, getPartitionSpec(updatePeriod,
      partitionTimestamps), latestPartCol);
  }

  /**
   * Add a partition to the fact on given storage for an updateperiod, with
   *  custom partition spec
   *
   * @param table The {@link CubeFactTable} object
   * @param storage The {@link Storage} object
   * @param updatePeriod The updatePeriod
   * @param partitionTimestamp partition timestamp
   * @param partSpec The partition spec
   * @param latestPartCol The partitoin column name if latest symlink has to be
   *        created, null otherwise
   * @throws HiveException
   */
  public void addPartition(CubeFactTable table, Storage storage,
      UpdatePeriod updatePeriod, Map<String, Date> partitionTimestamps,
      Map<String, String> partSpec, String latestPartCol)
          throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        table.getName(), storage.getPrefix());
    partSpec.putAll(getPartitionSpec(updatePeriod,
        partitionTimestamps));
    addPartition(storageTableName, storage, partSpec, latestPartCol);
  }

  /**
   * Add a partition to dimension table on a give storage
   *
   * @param table The {@link CubeDimensionTable} object
   * @param storage The {@link Storage} object
   * @param partitionTimestamp
   * @throws HiveException
   */
  public void addPartition(CubeDimensionTable table, Storage storage,
      Date partitionTimestamp) throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        table.getName(), storage.getPrefix());
    addPartition(storageTableName, storage, getPartitionSpec(table.
        getSnapshotDumpPeriods().get(storage.getName()), partitionTimestamp),
        Storage.getDatePartitionKey());
  }

  private Map<String, String> getPartitionSpec(
      UpdatePeriod updatePeriod, Date partitionTimestamp) {
    Map<String, String> partSpec = new HashMap<String, String>();
    SimpleDateFormat dateFormat = new SimpleDateFormat(updatePeriod.format());
    String pval = dateFormat.format(partitionTimestamp);
    partSpec.put(Storage.getDatePartitionKey(), pval);
    return partSpec;
  }

  private Map<String, String> getPartitionSpec(
      UpdatePeriod updatePeriod, Map<String, Date> partitionTimestamps) {
    Map<String, String> partSpec = new HashMap<String, String>();
    SimpleDateFormat dateFormat = new SimpleDateFormat(updatePeriod.format());
    for (Map.Entry<String, Date> entry : partitionTimestamps.entrySet()) {
      String pval = dateFormat.format(entry.getValue());
      partSpec.put(entry.getKey(), pval);
    }
    return partSpec;
  }

  private void addPartition(String storageTableName, Storage storage,
      Map<String, String> partSpec, String latestPartCol) throws HiveException {
    storage.addPartition(storageTableName, partSpec, config, latestPartCol);
  }

  boolean tableExists(String cubeName)
      throws HiveException {
    try {
      return (getClient().getTable(cubeName.toLowerCase(), false) != null);
    } catch (HiveException e) {
      throw new HiveException("Could not check whether table exists", e);
    }
  }

  boolean factPartitionExists(CubeFactTable fact,
      Storage storage, UpdatePeriod updatePeriod,
      Date partitionTimestamp) throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), storage.getPrefix());
    return partitionExists(storageTableName, updatePeriod, partitionTimestamp);
  }

  boolean factPartitionExists(CubeFactTable fact,
      Storage storage, UpdatePeriod updatePeriod,
      Map<String, Date> partitionTimestamp, Map<String, String> partSpec)
          throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), storage.getPrefix());
    return partitionExists(storageTableName, updatePeriod, partitionTimestamp,
        partSpec);
  }

  public boolean partitionExists(String storageTableName,
      UpdatePeriod updatePeriod,
      Map<String, Date> partitionTimestamps)
          throws HiveException {
    return partitionExists(storageTableName,
        getPartitionSpec(updatePeriod, partitionTimestamps));
  }

  public boolean partitionExistsByFilter(String storageTableName, String filter)
      throws HiveException {
    List<Partition> parts;
    try {
      parts = getClient().getPartitionsByFilter(
          getTable(storageTableName), filter);
    } catch (Exception e) {
      throw new HiveException("Could not find partitions for given filter", e);
    }
    return !(parts.isEmpty());
  }

  public List<Partition> getPartitionsByFilter(String storageTableName,
      String filter) throws MetaException, NoSuchObjectException,
      HiveException, TException {
    return getClient().getPartitionsByFilter(
      getTable(storageTableName), filter);
  }

  public int getNumPartitionsByFilter(String storageTableName,
      String filter) throws MetaException, NoSuchObjectException,
      HiveException, TException {
    return getClient().getNumPartitionsByFilter(
      getTable(storageTableName), filter);
  }

  public boolean partitionExists(String storageTableName,
      UpdatePeriod updatePeriod,
      Date partitionTimestamp)
          throws HiveException {
    return partitionExists(storageTableName,
        getPartitionSpec(updatePeriod, partitionTimestamp));
  }

  boolean partitionExists(String storageTableName, UpdatePeriod updatePeriod,
      Map<String, Date> partitionTimestamps, Map<String, String> partSpec)
          throws HiveException {
    partSpec.putAll(getPartitionSpec(updatePeriod, partitionTimestamps));
    return partitionExists(storageTableName, partSpec);
  }

  private boolean partitionExists(String storageTableName,
      Map<String, String> partSpec) throws HiveException {
    try {
      Table storageTbl = getTable(storageTableName);
      Partition p = getClient().getPartition(storageTbl, partSpec, false);
      return (p != null && p.getTPartition() != null);
    } catch (HiveException e) {
      throw new HiveException("Could not check whether table exists", e);
    }
  }

  boolean dimPartitionExists(CubeDimensionTable dim,
      Storage storage, Date partitionTimestamp) throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dim.getName(), storage.getPrefix());
    return partitionExists(storageTableName,
      dim.getSnapshotDumpPeriods().get(storage.getName()),
      partitionTimestamp);
  }

  boolean latestPartitionExists(CubeDimensionTable dim,
      Storage storage) throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dim.getName(), storage.getPrefix());
    return partitionExistsByFilter(storageTableName,
      Storage.getLatestPartFilter(Storage.getDatePartitionKey()));
  }

  boolean latestPartitionExists(CubeFactTable fact,
      Storage storage, String latestPartCol) throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), storage.getPrefix());
    return partitionExistsByFilter(storageTableName,
      Storage.getLatestPartFilter(latestPartCol));
  }

  /**
   * Get the hive {@link Table} corresponding to the name
   *
   * @param tableName
   * @return {@link Table} object
   * @throws HiveException
   */
  public Table getHiveTable(String tableName) throws HiveException {
    return getTable(tableName);
  }

  private Table getTable(String tableName) throws HiveException {
    Table tbl;
    try {
      tbl = allHiveTables.get(tableName.toLowerCase());
      if (tbl == null) {
        tbl = getClient().getTable(tableName.toLowerCase());
        if (enableCaching) {
          allHiveTables.put(tableName.toLowerCase(), tbl);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get table", e);
    }
    return tbl;
  }

  private Table getTable(String tableName, boolean refresh) throws HiveException {
    Table tbl;
    try {
      tbl = getClient().getTable(tableName.toLowerCase());
      allHiveTables.put(tableName.toLowerCase(), tbl);
    } catch (HiveException e) {
      throw new HiveException("Could not get table", e);
    }
    return tbl;
  }

  public void dropHiveTable(String table) throws HiveException {
    metastore.dropTable(table);
    allHiveTables.remove(table);
    allHiveTableNames.remove(table);
  }

  /**
   * Is the table name passed a fact table?
   *
   * @param tableName table name
   * @return true if it is cube fact, false otherwise
   * @throws HiveException
   */
  public boolean isFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isFactTable(tbl);
  }

  boolean isFactTable(Table tbl) {
    String tableType = tbl.getParameters().get(
      MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.FACT.name().equals(tableType);
  }

  boolean isFactTableForCube(Table tbl, String cube) {
    if (isFactTable(tbl)) {
      String cubeName = tbl.getParameters().get(
          MetastoreUtil.getFactCubeNameKey(tbl.getTableName()));
      return cubeName.equalsIgnoreCase(cube);
    }
    return false;
  }

  /**
   * Is the table name passed a dimension table?
   *
   * @param tableName table name
   * @return true if it is cube dimension, false otherwise
   * @throws HiveException
   */
  public boolean isDimensionTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isDimensionTable(tbl);
  }

  boolean isDimensionTable(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(
      MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.DIMENSION.name().equals(tableType);
  }

  /**
   * Is the table name passed a cube?
   *
   * @param tableName table name
   * @return true if it is cube, false otherwise
   * @throws HiveException
   */
  public boolean isCube(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isCube(tbl);
  }

  /**
   * Is the hive table a cube table?
   *
   * @param tbl
   * @return
   * @throws HiveException
   */
  boolean isCube(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(
      MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.CUBE.name().equals(tableType);
  }

  /**
   * Get {@link CubeFactTable} object corresponding to the name
   *
   * @param tableName The cube fact name
   * @return Returns CubeFactTable if table name passed is a fact table,
   *  null otherwise
   * @throws HiveException
   */

  public CubeFactTable getFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return getFactTable(tbl);
  }

  private CubeFactTable getFactTable(Table tbl) throws HiveException {
    if (isFactTable(tbl)) {
      return new CubeFactTable(tbl);
    }
    return null;
  }

  /**
   * Get {@link CubeDimensionTable} object corresponding to the name
   *
   * @param tableName The cube dimension name
   * @return Returns CubeDimensionTable if table name passed is a dimension
   *  table, null otherwise
   * @throws HiveException
   */
  public CubeDimensionTable getDimensionTable(String tableName)
      throws HiveException {
    CubeDimensionTable dimTable = allDims.get(tableName.toLowerCase());
    if (dimTable == null) {
      Table tbl = getTable(tableName);
      if (isDimensionTable(tbl)) {
        dimTable = getDimensionTable(tbl);
        if (enableCaching) {
          allDims.put(tableName.toLowerCase(), dimTable);
        }
      }
    }
    return dimTable;
  }

  private CubeDimensionTable getDimensionTable(Table tbl)
      throws HiveException {
      return new CubeDimensionTable(tbl);
  }

  /**
   * Get {@link Cube} object corresponding to the name
   *
   * @param tableName The cube name
   * @return Returns cube is table name passed is a cube, null otherwise
   * @throws HiveException
   */
  public Cube getCube(String tableName) throws HiveException {
    Cube cube = allCubes.get(tableName.toLowerCase());
    if (cube == null) {
      Table tbl = getTable(tableName);
      if (isCube(tbl)) {
        cube = getCube(tbl);
        if (enableCaching) {
          allCubes.put(tableName.toLowerCase(), cube);
        }
      }
    }
    return cube;
  }

  private Cube getCube(Table tbl) {
    return new Cube(tbl);
  }

  /**
   * Get all dimension tables in metastore
   *
   * @return List of dimension tables
   *
   * @throws HiveException
   */
  public List<CubeDimensionTable> getAllDimensionTables()
      throws HiveException {

    List<CubeDimensionTable> dimTables = new ArrayList<CubeDimensionTable>();
    try {
      for (String table : getAllHiveTableNames()) {
        CubeDimensionTable dim = getDimensionTable(table);
        if (dim != null) {
          dimTables.add(dim);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all tables", e);
    }
    return dimTables;
  }

  /**
   * Get all cubes in metastore
   *
   * @return List of Cube objects
   * @throws HiveException
   */
  public List<Cube> getAllCubes()
      throws HiveException {
    List<Cube> cubes = new ArrayList<Cube>();
    try {
      for (String table : getAllHiveTableNames()) {
        Cube cube = getCube(table);
        if (cube != null) {
          cubes.add(cube);
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all tables", e);
    }
    return cubes;
  }

  private List<String> getAllHiveTableNames() throws HiveException {
    if (allHiveTableNames.isEmpty()) {
      if (enableCaching) {
        allHiveTableNames.addAll(getClient().getAllTables());
      } else {
        return getClient().getAllTables();
      }
    }
    return allHiveTableNames;
  }

  /**
   * Get all fact tables in the cube.
   *
   * @param cube Cube object
   *
   * @return List of fact tables
   * @throws HiveException
   */
  public List<CubeFactTable> getAllFactTables(Cube cube) throws HiveException {
    List<CubeFactTable> factTables = allFactTables.get(cube);
    if (factTables == null) {
      factTables = new ArrayList<CubeFactTable>();
      try {
        for (String tableName : getAllHiveTableNames()) {
          Table tbl = getTable(tableName);
          if (isFactTableForCube(tbl, cube.getName())) {
            factTables.add(new CubeFactTable(tbl));
          }
        }
      } catch (HiveException e) {
        throw new HiveException("Could not get all tables", e);
      }
      if (enableCaching) {
        allFactTables.put(cube, factTables);
      }
    }
    return factTables;
  }

  public List<String> getPartColNames(String tableName)
      throws HiveException {
    List<String> partColNames = new ArrayList<String>();
    Table tbl = getTable(tableName);
    for (FieldSchema f : tbl.getPartCols()) {
      partColNames.add(f.getName().toLowerCase());
    }
    return partColNames;
  }

  public boolean partColExists(String tableName, String partCol)
      throws HiveException {
    Table tbl = getTable(tableName);
    for (FieldSchema f : tbl.getPartCols()) {
      if (f.getName().equalsIgnoreCase(partCol)) {
        return true;
      }
    }
    return false;
  }

  public void setSchemaGraph(SchemaGraph schemaGraph) {
    this.schemaGraph = schemaGraph;
  }

  public SchemaGraph getSchemaGraph() {
    return schemaGraph;
  }

  private void alterCubeTable(String table, Table hiveTable,
      AbstractCubeTable cubeTable) throws HiveException {
    hiveTable.getParameters().putAll(cubeTable.getProperties());
    hiveTable.getTTable().getSd().setCols(cubeTable.getColumns());
    hiveTable.getTTable().getParameters().putAll(cubeTable.getProperties());
    try {
      metastore.alterTable(table, hiveTable);
    } catch (InvalidOperationException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Alter cube specified by the name to new definition
   *
   * @param cubeName The cube name to be altered
   * @param cube The new cube definition
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterCube(String cubeName, Cube cube)
      throws HiveException {
    Table cubeTbl = getTable(cubeName);
    if (isCube(cubeTbl)) {
      alterCubeTable(cubeName, cubeTbl, cube);
      if (enableCaching) {
        allCubes.put(cubeName, getCube(getTable(cubeName, true)));
      }
    } else {
      throw new HiveException(cubeName + " is not a cube");
    }
  }

  /**
   * Drop a cube with cascade flag
   *
   * @param cubeName
   * @param cascade If true, will drop all facts of the cube and
   *  their storage tables
   * @throws HiveException
   */
  public void dropCube(String cubeName, boolean cascade) throws HiveException {
    if (isCube(cubeName)) {
      if (cascade) {
        Cube cube = getCube(cubeName);
        List<CubeFactTable> factTables = getAllFactTables(cube);
        for (CubeFactTable fact : factTables) {
          dropFact(fact.getName(), cascade);
        }
        allFactTables.remove(cube);
      }

      allCubes.remove(cubeName.toLowerCase());
      dropHiveTable(cubeName);
    } else {
      throw new HiveException(cubeName + " is not a cube");
    }
  }

  /**
   * Drop a fact with cascade flag
   *
   * @param factName
   * @param cascade If true, will drop all the storages of the fact
   * @throws HiveException
   */
  public void dropFact(String factName, boolean cascade) throws HiveException {
    if (isFactTable(factName)) {
      CubeFactTable fact = getFactTable(factName);
      if (cascade) {
        for (String storage : fact.getStorages()) {
          dropStorageFromFact(factName, storage);
        }
      }
      updateFactCache(factName);
      dropHiveTable(factName);
    } else {
      throw new HiveException(factName + " is not a CubeFactTable");
    }
  }

  /**
   * Drop a storage from fact
   *
   * @param factName
   * @param storage
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void dropStorageFromFact(String factName, String storage)
      throws HiveException {
    getFactTable(factName).dropStorage(storage);
    dropHiveTable(MetastoreUtil.getFactStorageTableName(factName,
      Storage.getPrefix(storage)));
    alterCubeTable(factName, getTable(factName), getFactTable(factName));
    updateFactCache(factName);
  }

  /**
   * Drop a storage from dimension
   *
   * @param dimName
   * @param storage
   * @throws HiveException
   */
  public void dropStorageFromDim(String dimName, String storage)
      throws HiveException {
    getDimensionTable(dimName).dropStorage(storage);
    dropHiveTable(MetastoreUtil.getDimStorageTableName(dimName,
      Storage.getPrefix(storage)));
    alterCubeTable(dimName, getTable(dimName), getDimensionTable(dimName));
    updateDimCache(dimName);
  }

  /**
   * Drop the dimension
   *
   * @param dimName
   * @param cascade If true, will drop all the dimension storages
   *
   * @throws HiveException
   */
  public void dropDimension(String dimName, boolean cascade)
      throws HiveException {
    if (isDimensionTable(dimName)) {
      CubeDimensionTable dim = getDimensionTable(dimName);
      Set<String> dimStorages = new HashSet<String>();
      dimStorages.addAll(dim.getStorages());
      for(String storage : dimStorages) {
        dropStorageFromDim(dimName, storage);
      }
      dropHiveTable(dimName);
      allDims.remove(dimName.toLowerCase());
    } else {
      throw new HiveException(dimName + " is not a dimension table");
    }
  }

  /**
   * Alter a cubefact with new definition
   *
   * @param factTableName
   * @param cubeFactTable
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterCubeFactTable(String factTableName,
      CubeFactTable cubeFactTable) throws  HiveException {
    Table factTbl = getTable(factTableName);
    if (isFactTable(factTbl)) {
      alterCubeTable(factTableName, factTbl, cubeFactTable);
      updateFactCache(factTableName);
    } else {
      throw new HiveException(factTableName + " is not a fact table");
    }
  }

  private void updateFactCache(String factTableName) throws HiveException {
    if (enableCaching) {
      Cube cube = getCube(
          getFactTable(getTable(factTableName, true)).getCubeName());
      if (cube != null) {
        // Remove all fact tables of the cube sothat it gets refreshed
        allFactTables.remove(cube);
      }
    }
  }

  private void updateDimCache(String dimName) throws HiveException {
    if (enableCaching) {
      allDims.put(dimName,
          getDimensionTable(getTable(dimName, true)));
    }
  }
  /**
   * Alter dimension table with new dimension definition
   *
   * @param dimTableName
   * @param cubeDimensionTable
   *
   * @throws HiveException
   * @throws InvalidOperationException
   */
  public void alterCubeDimensionTable(String dimTableName,
      CubeDimensionTable cubeDimensionTable) throws HiveException {
    Table dimTbl = getTable(dimTableName);
    if (isDimensionTable(dimTbl)) {
      alterCubeTable(dimTableName, dimTbl, cubeDimensionTable);
      updateDimCache(dimTableName);
    } else {
      throw new HiveException(dimTableName + " is not a dimension table");
    }
  }
}
