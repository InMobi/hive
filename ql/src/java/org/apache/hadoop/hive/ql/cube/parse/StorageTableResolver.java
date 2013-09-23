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
import java.util.TreeMap;
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
import org.apache.hadoop.hive.ql.metadata.Partition;
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
  private final Map<String, String> dimStorageTableToWhereClause =
      new HashMap<String, String>();
  private final List<String> nonExistingParts = new ArrayList<String>();
  private String processTimePartCol = null;
  private final UpdatePeriod maxInterval;

  public StorageTableResolver(Configuration conf) {
    this.conf = conf;
    this.supportedStorages = getSupportedStorages(conf);
    this.allStoragesSupported = (supportedStorages == null);
    this.failOnPartialData = conf.getBoolean(
        CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String str = conf.get(CubeQueryConfUtil.VALID_STORAGE_DIM_TABLES);
    validDimTables = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
    this.processTimePartCol = conf.get(CubeQueryConfUtil.PROCESS_TIME_PART_COL);
    String maxIntervalStr = conf.get(CubeQueryConfUtil.QUERY_MAX_INTERVAL);
    if (maxIntervalStr != null) {
      this.maxInterval = UpdatePeriod.valueOf(maxIntervalStr);
    } else {
      this.maxInterval = null;
    }
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
    cubeql.setStorageTableToWhereClause(dimStorageTableToWhereClause);
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
            dimStorageTableToWhereClause.put(tableName,
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
          new TreeMap<UpdatePeriod, Set<String>>();
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
          if (maxInterval != null && updatePeriod.compareTo(maxInterval) > 0) {
            LOG.info("Skipping update period " + updatePeriod + " for fact"
                + fact);
            continue;
          }
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

  private TreeSet<UpdatePeriod> getValidUpdatePeriods(CubeFactTable fact) {
    TreeSet<UpdatePeriod> set = new TreeSet<UpdatePeriod>();
    set.addAll(validStorageMap.get(fact).keySet());
    return set;
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
      for (TimeRange range : cubeql.getTimeRanges()) {
        Set<FactPartition> rangeParts = getPartitions(cfact.fact, range);
        if (rangeParts == null || rangeParts.isEmpty()) {
          continue;
        }
        cfact.numQueriedParts += rangeParts.size();
        answeringParts.addAll(rangeParts);
        cfact.rangeToWhereClause.put(range, StorageUtil.getWherePartClause(
            cubeql.getAliasForTabName(cfact.fact.getCubeName()), rangeParts));
      }
      if (cfact.numQueriedParts == 0) {
        LOG.info("Not considering the fact table:" + cfact.fact + " as it could"
            + " not find partition for given ranges: " + cubeql.getTimeRanges());
        i.remove();
        continue;
      }
      // Map from storage to covering parts
      Map<String, Set<FactPartition>> minimalStorageTables =
          new LinkedHashMap<String, Set<FactPartition>>();
      boolean enabledMultiTableSelect = StorageUtil.getMinimalAnsweringTables(
          answeringParts, minimalStorageTables);
      Set<String> storageTables = new LinkedHashSet<String>();
      storageTables.addAll(minimalStorageTables.keySet());
      cfact.storageTables = storageTables;
      // multi table select is already false, do not alter it
      if (cfact.enabledMultiTableSelect) {
        cfact.enabledMultiTableSelect = enabledMultiTableSelect;
      }
      LOG.info("Resolved partitions for fact " + cfact + ": " + answeringParts
          + " storageTables:" + storageTables);
    }
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range)
      throws SemanticException {
    try {
      return getPartitions(fact, range, getValidUpdatePeriods(fact), true);
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private Set<FactPartition> getPartitions(CubeFactTable fact, TimeRange range,
      TreeSet<UpdatePeriod> updatePeriods,
      boolean addNonExistingParts)
          throws Exception {
    Set<FactPartition> partitions = new TreeSet<FactPartition>();
    if (getPartitions(fact, range.getFromDate(), range.getToDate(),
        range.getPartitionColumn(), null,
        partitions, updatePeriods, addNonExistingParts)) {
      return partitions;
    } else {
      return null;
    }
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate,
      String partCol, FactPartition containingPart,
      Set<FactPartition> partitions,
      TreeSet<UpdatePeriod> updatePeriods,  boolean addNonExistingParts)
          throws Exception {
    LOG.info("getPartitions for " + fact + " from fromDate:" + fromDate
        + " toDate:" + toDate);
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }
    UpdatePeriod interval = CubeFactTable.maxIntervalInRange(fromDate, toDate,
        updatePeriods);
    if (interval == null) {
      return false;
    }
    LOG.info("Max interval for " + fact + " is:" + interval);
    Set<String> storageTbls = new LinkedHashSet<String>();
    storageTbls.addAll(validStorageMap.get(fact).get(interval));

    Iterator<String> it = storageTbls.iterator();
    while (it.hasNext()) {
      String storageTableName = it.next();
      if (!client.partColExists(storageTableName, partCol)) {
        LOG.info(partCol + " does not exist in" + storageTableName);
        it.remove();
        continue;
      }
      if (containingPart != null) {
        if (!client.partColExists(storageTableName, containingPart.partCol)) {
          LOG.info(partCol + " does not exist in" + storageTableName);
          it.remove();
        }
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
    long numIters = DateUtil.getTimeDiff(ceilFromDate, floorToDate, interval);
    int i = 1;
    int lookAheadNumParts = conf.getInt(
        CubeQueryConfUtil.getLookAheadPTPartsKey(interval),
        CubeQueryConfUtil.DEFAULT_LOOK_AHEAD_PT_PARTS);
    boolean leastInterval = updatePeriods.first().equals(interval);
    while (dt.compareTo(floorToDate) < 0) {
      cal.add(interval.calendarField(), 1);
      boolean foundPart = false;
      SimpleDateFormat pformat = new SimpleDateFormat(fmt);
      FactPartition part = new FactPartition(partCol,
          pformat.format(dt), interval, containingPart);
      Map<String, List<Partition>> metaParts = new HashMap<String, List<Partition>>();
      for (String storageTableName : storageTbls) {
        int numParts;
        if (leastInterval) {
          numParts = client.getNumPartitionsByFilter(
              storageTableName, part.getFilter(null));
        } else {
          List<Partition> sParts = client.getPartitionsByFilter(
              storageTableName, part.getFilter(null));
          metaParts.put(storageTableName, sParts);
          numParts = sParts.size();
        }
        if (numParts > 0) {
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
      if (containingPart == null) {
        if (!foundPart) {
          LOG.info("Partition:" + part + " does not exist in any storage table");
          TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
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
        } else if (processTimePartCol != null) {
          LOG.info("Looking for look ahead process time partitions for "+ part);
          if (!partCol.equals(processTimePartCol)) {
            if (!leastInterval) {
              // see if this is the part of the last-n look ahead partitions
              if ((numIters - i) <= lookAheadNumParts) {
                LOG.info("Looking for look ahead process time partitions for "+ part);
                // check if finer partitions are required
                // final partitions are required if no partitions from look-ahead
                // process time are present
                Calendar processCal = Calendar.getInstance();
                processCal.setTime(cal.getTime());
                Date start =  processCal.getTime();
                processCal.add(interval.calendarField(), lookAheadNumParts);
                Date end = processCal.getTime();
                Calendar temp = Calendar.getInstance();
                temp.setTime(start);
                while (temp.getTime().compareTo(end) < 0) {
                  Date pdt = temp.getTime();
                  String lPart = pformat.format(pdt);
                  temp.add(interval.calendarField(), 1);
                  Boolean foundLookAheadParts = false;
                  for (Map.Entry<String, List<Partition>> entry : metaParts.entrySet()) {
                    for (Partition mpart :entry.getValue()) {
                      if (mpart.getValues().get(0).contains(lPart)) {
                        LOG.info("Founr lPart in " + mpart + " in table:" + entry.getKey());
                        foundLookAheadParts = true;
                        break;
                      }
                    }
                  }
                  if (!foundLookAheadParts) {
                    LOG.info("Looked ahead process time partition " + lPart + " is not found");
                    TreeSet<UpdatePeriod> newset = new TreeSet<UpdatePeriod>();
                    newset.addAll(updatePeriods);
                    newset.remove(interval);
                    LOG.info("newset of update periods:" + newset);
                    if (!newset.isEmpty()) {
                      // Get partitions for look ahead process time
                      Set<FactPartition> processTimeParts = new TreeSet<FactPartition>();
                      LOG.info("Looking for process time partitions between " + pdt + " and " + temp.getTime());
                      getPartitions(fact, pdt, temp.getTime(),
                          processTimePartCol, null, processTimeParts,
                          newset, false);
                      if (!processTimeParts.isEmpty()) {
                        for (FactPartition pPart : processTimeParts) {
                          LOG.info("Looking for finer partitions in pPart" + pPart);
                          if (!getPartitions(fact, dt, cal.getTime(), partCol, pPart,
                              partitions, newset, false)) {
                            LOG.info("No partitions found in look ahead range");
                          }
                        }
                      } else {
                        LOG.info("No look ahead partitions found");
                      }
                    }
                  } else {
                    LOG.info("Finer parts not required for look-ahead partition :" + part);
                  }
                }
              } else {
                LOG.info("Not a look ahead partition");
              }
            } else {
              LOG.info("Update period is the least update period");
            }
          } else {
            LOG.info("part column is process time col");
          }
        }
      }
      dt = cal.getTime();
      i++;
    }
    if (containingPart == null) {
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
