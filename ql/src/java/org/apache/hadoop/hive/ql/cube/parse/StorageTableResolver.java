package org.apache.hadoop.hive.ql.cube.parse;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateFact;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class StorageTableResolver implements ContextRewriter {
  private static Log LOG = LogFactory.getLog(
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
  private final Map<CubeDimensionTable, List<String>> dimStorageMap =
      new HashMap<CubeDimensionTable, List<String>>();
  private final Map<String, String> storageTableToWhereClause =
      new HashMap<String, String>();
  private final List<String> nonExistingParts = new ArrayList<String>();

  public StorageTableResolver(Configuration conf) {
    this.conf = conf;
    this.supportedStorages = getSupportedStorages(conf);
    this.allStoragesSupported = (supportedStorages == null);
    this.failOnPartialData = conf.getBoolean(
        CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String str = conf.get(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES);
    validDimTables = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
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

    if (!cubeql.getCandidateFactTables().isEmpty()) {
      // resolve storage table names
      resolveFactStorageTableNames(cubeql);
      // resolve storage partitions
      resolveFactStoragePartitions(cubeql);
    }
    // resolve dimension tables
    resolveDimStorageTablesAndPartitions(cubeql);
    cubeql.setDimStorageMap(dimStorageMap);

    // set storage to whereclause
    cubeql.setStorageTableToWhereClause(storageTableToWhereClause);
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
                StorageUtil.getWherePartClause(Storage.getDatePartitionKey(),
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
    for (Iterator<CandidateFact> i =
        cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CubeFactTable fact = i.next().fact;
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
    // Find candidate tables wrt supported storages
    for (Iterator<CandidateFact> i =
        cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
      CandidateFact cfact = i.next();
      List<FactPartition> answeringParts = new ArrayList<FactPartition>();
      boolean considerFact = true;
      for (TimeRange range : cubeql.getTimeRanges()) {
        List<FactPartition> rangeParts = getPartitions(cfact.fact, range);
        if (rangeParts == null || rangeParts.isEmpty()) {
          LOG.info("The range:" + range + "is not answerable by "+ cfact.fact);
          considerFact = false;
          break;
        }
        answeringParts.addAll(rangeParts);
      }
      if (!considerFact) {
        LOG.info("Not considering the fact table:" + cfact.fact + " as it could not" +
            " find partition for given ranges: " + cubeql.getTimeRanges());
        i.remove();
        continue;
      }
      cfact.numQueriedParts = answeringParts.size();

      // Map from storage to covering parts
      Map<String, Set<FactPartition>> minimalStorageTables =
          new LinkedHashMap<String, Set<FactPartition>>();
      boolean enabledMultiTableSelect = StorageUtil.getMinimalAnsweringTables(
          answeringParts, minimalStorageTables);
      Set<String> storageTables = new LinkedHashSet<String>();

      for (Map.Entry<String, Set<FactPartition>> entry : minimalStorageTables
          .entrySet()) {
        storageTables.add(entry.getKey());

        LOG.info("For fact:" + cfact.fact
            + " Parts:" + entry.getValue() + " storageTable:" + entry.getKey());
        storageTableToWhereClause.put(entry.getKey(),
            StorageUtil.getWherePartClause(
            cubeql.getAliasForTabName(cfact.fact.getCubeName()), entry.getValue()));
      }
      cfact.storageTables = storageTables;
      // multi table select is already false, do not alter it
      if (cfact.enabledMultiTableSelect) {
        cfact.enabledMultiTableSelect = enabledMultiTableSelect;
      }
    }
  }

  private List<FactPartition> getPartitions(CubeFactTable fact, TimeRange range)
      throws SemanticException {
    try {
      return getPartitions(fact, range, null, getValidUpdatePeriods(fact), true);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private List<FactPartition> getPartitions(CubeFactTable fact, TimeRange range,
      List<FactPartition> containingParts, Set<UpdatePeriod> updatePeriods,
      boolean addNonExistingParts)
          throws Exception {
    List<FactPartition> partitions = new ArrayList<FactPartition>();
    if (getPartitions(fact, range.getFromDate(), range.getToDate(),
        range.getPartitionColumn(), containingParts,
        partitions, updatePeriods, addNonExistingParts)) {
      System.out.println("Partitions returned:" + partitions);
      if (range.getChild() != null) {
        List<FactPartition> rangeParts = new ArrayList<FactPartition>();
        for (FactPartition part : partitions) {
          System.out.println("Looking for child partitions in:" + part);
          List<FactPartition> childContParts = new ArrayList<FactPartition>();
          if (containingParts != null) {
            childContParts.addAll(containingParts);
          }
          childContParts.add(part);
          List<FactPartition> childParts = getPartitions(fact,
              range.getChild(), childContParts,
              updatePeriods, addNonExistingParts);
          if (childParts != null) {
            rangeParts.addAll(childParts);
          }
        }
        return rangeParts;
      } else {
        // This is the final range in nested range
        // return the partitions to to-be queried partitions
        return partitions;
      }
    } else {
      return null;
    }
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate,
      String partCol, List<FactPartition> containingParts,
      List<FactPartition> partitions,
      Set<UpdatePeriod> updatePeriods,  boolean addNonExistingParts)
          throws Exception {
    LOG.info("getPartitions for " + fact + " from fromDate:" + fromDate
        + " toDate:" + toDate);
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }
    UpdatePeriod interval;
    Set<String> storageTbls = new LinkedHashSet<String>();
    if (containingParts != null) {
      interval = containingParts.get(containingParts.size() - 1).period;
      storageTbls.addAll(containingParts.get(containingParts.size() - 1).storageTables);
    } else {
      interval = CubeFactTable.maxIntervalInRange(fromDate, toDate,
          updatePeriods);
      if (interval == null) {
        return false;
      }
      LOG.info("Max interval for " + fact + " is:" + interval);
      storageTbls.addAll(validStorageMap.get(fact).get(interval));
    }

    Iterator<String> it = storageTbls.iterator();
    while (it.hasNext()) {
      String storageTableName = it.next();
      if (!client.partColExists(storageTableName, partCol)) {
        LOG.info(partCol + " does not exist in" + storageTableName);
        it.remove();
      }
    }

    if (storageTbls.isEmpty()) {
      return false;
    }

    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);

    // add partitions from ceilFrom to floorTo
    String fmt = interval.format();
    Calendar cal = Calendar.getInstance();
    cal.setTime(ceilFromDate);
    Date dt = cal.getTime();
    while (dt.compareTo(floorToDate) < 0) {
      cal.add(interval.calendarField(), 1);
      boolean foundPart = false;
      FactPartition part = new FactPartition(partCol,
          new SimpleDateFormat(fmt).format(dt), interval, containingParts);
      for (String storageTableName : storageTbls) {
        if (client.partitionExistsByFilter(storageTableName, part.getFilter(
            null))) {
          if (!foundPart) {
            LOG.info("Adding existing partition" + part);
            partitions.add(part);
            foundPart = true;
          }
          part.storageTables.add(storageTableName);
        } else {
          LOG.info("Partition " + part + " does not exist on " + storageTableName);
        }
      }
      if (!foundPart && containingParts == null) {
        LOG.info("Partition:" + part + " does not exist in any storage table");
        Set<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
        newset.addAll(updatePeriods);
        newset.remove(interval);
        if (!getPartitions(fact, dt, cal.getTime(), partCol, null, partitions,
            newset, false)) {
          if (!failOnPartialData && addNonExistingParts) {
            LOG.info("Adding non existing partition" + part);
            partitions.add(part);
            nonExistingParts.add(part.partSpec);
            foundPart = true;
            // add all storage tables as the answering tables
            part.storageTables.addAll(storageTbls);
          } else {
            LOG.info("No finer granual partitions exist for" + part);
            return false;
          }
        } else {
          LOG.info("Finer granual partitions added for " + part);
        }
      }
      dt = cal.getTime();
    }
    if (containingParts == null) {
      return
        (getPartitions(fact, fromDate, ceilFromDate, partCol, null, partitions,
           updatePeriods, addNonExistingParts) &&
         getPartitions(fact, floorToDate, toDate, partCol, null, partitions,
           updatePeriods, addNonExistingParts));
    } else {
      return true;
    }
  }
}
