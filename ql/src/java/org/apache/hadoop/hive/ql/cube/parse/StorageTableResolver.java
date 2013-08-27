package org.apache.hadoop.hive.ql.cube.parse;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimensionTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.MetastoreUtil;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class StorageTableResolver implements ContextRewriter {
  public static Log LOG = LogFactory.getLog(
      StorageTableResolver.class.getName());

  private final Configuration conf;
  private final List<String> supportedStorages;
  private final boolean allStoragesSupported;
  CubeMetastoreClient client;
  private final boolean failOnPartialData;
  private final List<String> validDimTables;
  private final Map<CubeFactTable, Map<UpdatePeriod, Set<String>>>
  validStorageMap =
  new HashMap<CubeFactTable, Map<UpdatePeriod, Set<String>>>();
  private final Map<CubeFactTable, Integer> factPartMap =
      new HashMap<CubeFactTable, Integer>();
  private final Map<CubeFactTable, Set<String>> factStorageMap =
      new HashMap<CubeFactTable, Set<String>>();
  private final Map<CubeDimensionTable, List<String>> dimStorageMap =
      new HashMap<CubeDimensionTable, List<String>>();
  private final Map<String, String> storageTableToWhereClause =
      new HashMap<String, String>();
  private final List<String> nonExistingParts = new ArrayList<String>();
  private boolean enableMultiTableSelect = true;
  private String timePartitionColumn = null;

  public StorageTableResolver(Configuration conf) {
    this.conf = conf;
    this.supportedStorages = getSupportedStorages(conf);
    this.allStoragesSupported = (supportedStorages == null);
    this.failOnPartialData = conf.getBoolean(
        CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String str = conf.get(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES);
    validDimTables = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
    this.enableMultiTableSelect = conf.getBoolean(
        CubeQueryConfUtil.ENABLE_MULTI_TABLE_SELECT,
        CubeQueryConfUtil.DEFAULT_MULTI_TABLE_SELECT);
  }

  private List<String> getSupportedStorages(Configuration conf) {
    String[] storages = conf.getStrings(
        CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES);
    if (storages != null) {
      return Arrays.asList(storages);
    }
    return null;
  }

  public boolean isStorageSupported(String storage) {
    if (!allStoragesSupported) {
      return supportedStorages.contains(storage);
    }
    return true;
  }

  Map<String, List<String>> storagePartMap =
      new HashMap<String, List<String>>();

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    client = cubeql.getMetastoreClient();
    if (!cubeql.getTimeRanges().isEmpty()) {
      timePartitionColumn = cubeql.getTimeRanges().get(0).getPartitionColumn();
    }

    if (!cubeql.getCandidateFactTables().isEmpty()) {
      // resolve storage table names
      resolveFactStorageTableNames(cubeql);
      // resolve storage partitions
      resolveFactStoragePartitions(cubeql);
      cubeql.setFactStorageMap(factStorageMap);
      cubeql.setFactPartitionMap(factPartMap);
    }
    // resolve dimension tables
    resolveDimStorageTablesAndPartitions(cubeql);
    cubeql.setDimStorageMap(dimStorageMap);

    // set storage to whereclause
    cubeql.setStorageTableToWhereClause(storageTableToWhereClause);
    cubeql.setMultiTableSelect(enableMultiTableSelect);
    cubeql.setNonexistingParts(nonExistingParts);
  }

  private void resolveDimStorageTablesAndPartitions(CubeQueryContext cubeql) {
    for (CubeDimensionTable dim : cubeql.getDimensionTables()) {
      for (String storage : dim.getStorages()) {
        if (isStorageSupported(storage)) {
          String tableName = MetastoreUtil.getDimStorageTableName(
              dim.getName(), Storage.getPrefix(storage)).toLowerCase();
          if (validDimTables != null && !validDimTables.contains(tableName)) {
            LOG.info("Not considering the dim storage table:" + tableName
                + " as it is not a valid dim storage");
            continue;
          }
          List<String> storageTables = dimStorageMap.get(dim);
          if (storageTables == null) {
            storageTables = new ArrayList<String>();
            dimStorageMap.put(dim, storageTables);
          }
          storageTables.add(tableName);
          if (dim.hasStorageSnapshots(storage)) {
            storageTableToWhereClause.put(tableName,
                getWherePartClause(Storage.getDatePartitionKey(),
                    cubeql.getAliasForTabName(dim.getName()),
                    Storage.getPartitionsForLatest()));
          }
        } else {
          LOG.info("Storage:" + storage + " is not supported");
        }
      }
    }
  }

  //Resolves all the storage table names, which are valid for each updatePeriod
  private void resolveFactStorageTableNames(CubeQueryContext cubeql)
      throws SemanticException {
    for (Iterator<CubeFactTable> i =
        cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CubeFactTable fact = i.next();
      Map<UpdatePeriod, Set<String>> storageTableMap =
          new HashMap<UpdatePeriod, Set<String>>();
      validStorageMap.put(fact, storageTableMap);
      String str = conf.get(CubeQueryConfUtil.getValidStorageTablesKey(
          fact.getName()));
      List<String> validFactStorageTables = StringUtils.isBlank(str) ? null :
        Arrays.asList(StringUtils.split(str.toLowerCase(), ","));

      for (Map.Entry<String, Set<UpdatePeriod>> entry : fact
          .getUpdatePeriods().entrySet()) {
        String storage = entry.getKey();
        // skip storages that are not supported
        if (!isStorageSupported(storage)) {
          LOG.info("Skipping storage: " + storage + " as it is not supported");
          continue;
        }
        String tableName;
        // skip the update period if the storage is not valid
        if ((tableName = getStorageTableName(fact, storage,
            validFactStorageTables))
            == null) {
          continue;
        }
        try {
          if (!timerangeColsExist(tableName, cubeql.getTimeRanges())) {
            LOG.info("Skipping storage table " + tableName + " as it is not" +
                " partitioned by queried timerange columns");
            continue;
          }
        } catch (HiveException e) {
          LOG.warn("Could not find the table:" + tableName, e);
          throw new SemanticException("Could not find the table:" + tableName);
        }
        List<String> validUpdatePeriods = CubeQueryConfUtil.getStringList(conf,
            CubeQueryConfUtil.getValidUpdatePeriodsKey(fact.getName(), storage));

        for (UpdatePeriod updatePeriod : entry.getValue()) {
          if (validUpdatePeriods != null && !validUpdatePeriods
              .contains(updatePeriod.name().toLowerCase())) {
            LOG.info("Skipping update period " + updatePeriod + " for fact"
                + fact + " for storage" + storage);
            continue;
          }
          Set<String> storageTables = storageTableMap.get(updatePeriod);
          if (storageTables == null) {
            storageTables = new LinkedHashSet<String>();
            storageTableMap.put(updatePeriod, storageTables);
          }
          LOG.info("Adding storage table:" + tableName + " for fact:"+ fact +
              " for update period" + updatePeriod);
          storageTables.add(tableName);
        }
      }
      if (storageTableMap.isEmpty()) {
        LOG.info("Not considering the fact table:" + fact + " as it does not" +
            " have any storage tables");
        i.remove();
      }
    }
  }

  private boolean timerangeColsExist(String tableName, List<TimeRange> ranges)
      throws HiveException {
    for (TimeRange range : ranges) {
      if (!timerangeColsExist(tableName, range)) {
        return false;
      }
    }
    return true;
  }

  private boolean timerangeColsExist(String tableName, TimeRange range)
      throws HiveException {
      if (!client.partColExists(tableName, range.getPartitionColumn())) {
        LOG.info(range.getPartitionColumn() + " does not exist in" + tableName);
        return false;
      }
      if (range.getChild() != null) {
        return timerangeColsExist(tableName, range.getChild());
      }
    return true;
  }

  private Set<UpdatePeriod> getValidUpdatePeriods(CubeFactTable fact) {
    return validStorageMap.get(fact).keySet();
  }

  String getStorageTableName(CubeFactTable fact, String storage,
      List<String> validFactStorageTables) {
    String tableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), Storage.getPrefix(storage)).toLowerCase();
    if (validFactStorageTables != null && !validFactStorageTables
        .contains(tableName)) {
      LOG.info("Skipping storage table " + tableName + " as it is not valid");
      return null;
    }
    return tableName;
  }

  private void resolveFactStoragePartitions(CubeQueryContext cubeql)
      throws SemanticException {
    TimeRange range = cubeql.getTimeRanges().get(0);
    Date fromDate = range.getFromDate();
    Date toDate = range.getToDate();

    // Find candidate tables wrt supported storages
    for (Iterator<CubeFactTable> i =
        cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CubeFactTable fact = i.next();
      Map<String, Set<String>> answeringTablesMap =
          new HashMap<String, Set<String>>();
      Map<UpdatePeriod, Set<String>> updatePeriodPartitionMap = new HashMap<UpdatePeriod, Set<String>>();
      if (!getPartitionColMap(fact, answeringTablesMap, fromDate, toDate) ||
          answeringTablesMap.isEmpty()) {
        LOG.info("Not considering the fact table:" + fact + " as it could not" +
            " find partition for given range:" + fromDate + " - " + toDate);
        i.remove();
        continue;
      }
      factPartMap.put(fact, answeringTablesMap.keySet().size());

      // Map from storage to covering parts
      Map<String, Set<String>> minimalStorageTables = new LinkedHashMap<String, Set<String>>();
      boolean enabledMultiTableSelect = getMinimalAnsweringTables(
          answeringTablesMap, minimalStorageTables);
      Set<String> storageTables = new LinkedHashSet<String>();

      for (Map.Entry<String, Set<String>> entry : minimalStorageTables
          .entrySet()) {
        List<String> parts = new ArrayList<String>();
        parts.addAll(entry.getValue());
        storageTables.add(entry.getKey());

        LOG.info("For fact:" + fact
            + " Parts:" + parts + " storageTable:" + entry.getKey());
        storageTableToWhereClause.put(entry.getKey(), getWherePartClause(
            range.getPartitionColumn(),
            cubeql.getAliasForTabName(fact.getCubeName()), parts));
      }
      factStorageMap.put(fact, storageTables);
    }
  }

  /**
   * Get minimal set of storages which cover the queried partitions
   *
   * @param answeringTablesMap Map from partition to set of answering storage tables
   * @param Map from storage to covering parts
   *
   * @return true if multi table select is enabled, false otherwise
   */
  boolean getMinimalAnsweringTables(
      Map<String, Set<String>> answeringTablesMap,
      Map<String, Set<String>> minimalAnsweringTables) {
    // map from storage table to the partitions it covers
    Map<String, Set<String>> invertedMap =
        new HashMap<String, Set<String>>();
    boolean multiTableSelect = true;
    // invert the answering tables map and put in inverted map
    for (Map.Entry<String, Set<String>> entry : answeringTablesMap.entrySet()) {
      for (String table : entry.getValue()) {
        Set<String> periodsCovered = invertedMap.get(table);
        if (periodsCovered == null) {
          periodsCovered = new TreeSet<String>();
          invertedMap.put(table, periodsCovered);
        }
        periodsCovered.add(entry.getKey());
      }
    }
    // there exist only one storage
    if (invertedMap.size() != 1) {
      Set<String> queriedParts = answeringTablesMap.keySet();
      Set<String> remaining = new TreeSet<String>();
      remaining.addAll(queriedParts);
      while (!remaining.isEmpty()) {
        // returns a singleton map
        Map<String, Set<String>> maxCoveringStorage = getMaxCoveringStorage(
            invertedMap, remaining);
        minimalAnsweringTables.putAll(maxCoveringStorage);
        Set<String> coveringSet = maxCoveringStorage.values().iterator().next();
        if (enableMultiTableSelect) {
          if (!coveringSet.containsAll(invertedMap.get(
              maxCoveringStorage.keySet().iterator().next()))) {
            LOG.info("Disabling multi table select because the partitions are" +
                " not mutually exclusive");
            multiTableSelect = false;
            enableMultiTableSelect = false;
          }
        }
        remaining.removeAll(coveringSet);
      }
    } else {
      minimalAnsweringTables.putAll(invertedMap);
    }
    return multiTableSelect;
  }

  private Map<String, Set<String>> getMaxCoveringStorage(
      final Map<String, Set<String>> storageCoveringMap,
      Set<String> queriedParts) {
    int coveringcount = 0;
    int maxCoveringCount = 0;
    String maxCoveringStorage = null;
    Set<String> maxCoveringSet = null;
    for (Map.Entry<String, Set<String>> entry : storageCoveringMap
        .entrySet()) {
      Set<String> coveringSet = new TreeSet<String>();
      coveringSet.addAll(entry.getValue());
      coveringSet.retainAll(queriedParts);
      coveringcount = coveringSet.size();
      if (coveringcount > maxCoveringCount) {
        maxCoveringCount = coveringcount;
        maxCoveringStorage = entry.getKey();
        maxCoveringSet = coveringSet;
      }
    }
    return Collections.singletonMap(maxCoveringStorage, maxCoveringSet);
  }

  private boolean getPartitionColMap(CubeFactTable fact,
      Map<String, Set<String>> answeringTablesMap, Date fromDate,
      Date toDate) throws SemanticException {
    Set<UpdatePeriod> updatePeriods = getValidUpdatePeriods(fact);
    LOG.info("Valid update periods for fact" + fact + " are " + updatePeriods);
    try {
      return getPartitions(fact, fromDate, toDate,
          answeringTablesMap,
          updatePeriods, true);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate,
      Map<String, Set<String>> answeringTablesMap,
      Set<UpdatePeriod> updatePeriods, boolean addNonExistingParts)
          throws Exception {
    LOG.info("getPartitions for " + fact + " from fromDate:" + fromDate
        + " toDate:" + toDate);
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }

    UpdatePeriod interval = CubeFactTable.maxIntervalInRange(fromDate, toDate,
        updatePeriods);
    LOG.info("Max interval for " + fact + " is:" + interval);
    if (interval == null) {
      return false;
    }

    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);
    Set<String> storageTbls = validStorageMap.get(fact).get(interval);

    // add partitions from ceilFrom to floorTo
    String fmt = interval.format();
    Calendar cal = Calendar.getInstance();
    cal.setTime(ceilFromDate);
    List<String> partitions = new ArrayList<String>();
    Date dt = cal.getTime();
    while (dt.compareTo(floorToDate) < 0) {
      Set<String> answeringTables = new LinkedHashSet<String>();
      String part = new SimpleDateFormat(fmt).format(cal.getTime());
      cal.add(interval.calendarField(), 1);
      boolean foundPart = false;
      for (String storageTableName : storageTbls) {
        String filter = timePartitionColumn + "='" + part + "'";
        if (client.partitionExistsByFilter(storageTableName, filter)) {
          if (!foundPart) {
            LOG.info("Adding existing partition" + part);
            partitions.add(part);
            foundPart = true;
          }
          answeringTables.add(storageTableName);
        } else {
          LOG.info("Partition " + part + " does not exist on " + storageTableName);
        }
      }
      if (!foundPart) {
        LOG.info("Partition:" + part + " does not exist in any storage table");
        Set<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
        newset.addAll(updatePeriods);
        newset.remove(interval);
        if (!getPartitions(fact, dt, cal.getTime(), answeringTablesMap, newset,
            false)) {
          if (!failOnPartialData && addNonExistingParts) {
            LOG.info("Adding non existing partition" + part);
            partitions.add(part);
            nonExistingParts.add(part);
            foundPart = true;
            // add all storage tables as the answering tables
            answeringTables.addAll(storageTbls);
          } else {
            LOG.info("No finer granual partitions exist for" + part);
            return false;
          }
        } else {
          LOG.info("Finer granual partitions added for " + part);
        }
      }
      dt = cal.getTime();
      Set<String> tables = answeringTablesMap.get(part);
      if (tables == null) {
        tables = new LinkedHashSet<String>();
        answeringTablesMap.put(part, tables);
      }
      LOG.info("Adding storagetables" + answeringTables + " for fact:" + fact
          + "for part:"+ part);
      tables.addAll(answeringTables);
    }
    return (getPartitions(fact, fromDate, ceilFromDate,
        answeringTablesMap, updatePeriods, addNonExistingParts) &&
        getPartitions(fact, floorToDate, toDate,
            answeringTablesMap, updatePeriods, addNonExistingParts));
  }

  public static String getWherePartClause(String timeDimName,
      String tableName, List<String> parts) {
    if (parts.size() == 0) {
      return "";
    }
    StringBuilder partStr = new StringBuilder();
    for (int i = 0; i < parts.size() - 1; i++) {
      partStr.append(tableName);
      partStr.append(".");
      partStr.append(timeDimName);
      partStr.append(" = '");
      partStr.append(parts.get(i));
      partStr.append("'");
      partStr.append(" OR ");
    }

    // add the last partition
    partStr.append(tableName);
    partStr.append(".");
    partStr.append(timeDimName);
    partStr.append(" = '");
    partStr.append(parts.get(parts.size() - 1));
    partStr.append("'");
    return partStr.toString();
  }
}
