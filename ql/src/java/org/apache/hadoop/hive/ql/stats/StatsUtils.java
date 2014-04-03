/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class StatsUtils {

  private static final Log LOG = LogFactory.getLog(StatsUtils.class.getName());

  /**
   * Collect table, partition and column level statistics
   * @param conf
   *          - hive configuration
   * @param partList
   *          - partition list
   * @param table
   *          - table
   * @param tableScanOperator
   *          - table scan operator
   * @return statistics object
   * @throws HiveException
   */
  public static Statistics collectStatistics(HiveConf conf, PrunedPartitionList partList,
      Table table, TableScanOperator tableScanOperator) {

    Statistics stats = new Statistics();

    // column level statistics are required only for the columns that are needed
    List<ColumnInfo> schema = tableScanOperator.getSchema().getSignature();
    List<String> neededColumns = tableScanOperator.getNeededColumns();
    boolean fetchColStats =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_FETCH_COLUMN_STATS);
    boolean fetchPartStats =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_FETCH_PARTITION_STATS);
    float deserFactor =
        HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_DESERIALIZATION_FACTOR);

    if (!table.isPartitioned()) {
      long nr = getNumRows(table);
      long ds = getRawDataSize(table);
      if (ds <= 0) {
        ds = getTotalSize(table);

        // if data size is still 0 then get file size
        if (ds <= 0) {
          ds = getFileSizeForTable(conf, table);
        }

        ds = (long) (ds * deserFactor);
      }

      // number of rows -1 means that statistics from metastore is not reliable
      // and 0 means statistics gathering is disabled
      if (nr <= 0) {
        int avgRowSize = estimateRowSizeFromSchema(conf, schema, neededColumns);
        if (avgRowSize > 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Estimated average row size: " + avgRowSize);
          }
          nr = ds / avgRowSize;
        }
      }
      stats.setNumRows(nr);
      stats.setDataSize(ds);

      List<ColStatistics> colStats = Lists.newArrayList();
      if (fetchColStats) {
        colStats = getTableColumnStats(table, schema, neededColumns);
      }

      stats.setColumnStatsState(deriveStatType(colStats, neededColumns));
      stats.addToColumnStats(colStats);
    } else if (partList != null) {
      // For partitioned tables, get the size of all the partitions after pruning
      // the partitions that are not required
      long nr = 0;
      long ds = 0;

      List<Long> rowCounts = Lists.newArrayList();
      List<Long> dataSizes = Lists.newArrayList();

      if (fetchPartStats) {
        rowCounts = getBasicStatForPartitions(
            table, partList.getNotDeniedPartns(), StatsSetupConst.ROW_COUNT);
        dataSizes =  getBasicStatForPartitions(
            table, partList.getNotDeniedPartns(), StatsSetupConst.RAW_DATA_SIZE);

        nr = getSumIgnoreNegatives(rowCounts);
        ds = getSumIgnoreNegatives(dataSizes);
        if (ds <= 0) {
          dataSizes = getBasicStatForPartitions(
              table, partList.getNotDeniedPartns(), StatsSetupConst.TOTAL_SIZE);
          ds = getSumIgnoreNegatives(dataSizes);
        }
      }

      // if data size still could not be determined, then fall back to filesytem to get file
      // sizes
      if (ds <= 0) {
        dataSizes = getFileSizeForPartitions(conf, partList.getNotDeniedPartns());
      }
      ds = getSumIgnoreNegatives(dataSizes);
      ds = (long) (ds * deserFactor);

      int avgRowSize = estimateRowSizeFromSchema(conf, schema, neededColumns);
      if (avgRowSize > 0) {
        setUnknownRcDsToAverage(rowCounts, dataSizes, avgRowSize);
        nr = getSumIgnoreNegatives(rowCounts);
        ds = getSumIgnoreNegatives(dataSizes);

        // number of rows -1 means that statistics from metastore is not reliable
        if (nr <= 0) {
          nr = ds / avgRowSize;
        }
      }
      stats.addToNumRows(nr);
      stats.addToDataSize(ds);

      // if at least a partition does not contain row count then mark basic stats state as PARTIAL
      if (containsNonPositives(rowCounts)) {
        stats.setBasicStatsState(State.PARTIAL);
      }
      boolean haveFullStats = fetchColStats;
      if (fetchColStats) {
        List<String> partNames = new ArrayList<String>(partList.getNotDeniedPartns().size());
        for (Partition part : partList.getNotDeniedPartns()) {
          partNames.add(part.getName());
        }
        Map<String, List<ColStatistics>> partStats =
            getPartColumnStats(table, schema, partNames, neededColumns);
        if (partStats != null) {
          for (String partName : partNames) {
            List<ColStatistics> partStat = partStats.get(partName);
            haveFullStats &= (partStat != null);
            if (partStat != null) {
              stats.updateColumnStatsState(deriveStatType(partStat, neededColumns));
              stats.addToColumnStats(partStat);
            }
          }
        }
      }
      // There are some partitions with no state (or we didn't fetch any state).
      // Update the stats with empty list to reflect that in the state/initialize structures.
      if (!haveFullStats) {
        List<ColStatistics> emptyStats = Lists.<ColStatistics>newArrayList();
        stats.addToColumnStats(emptyStats);
        stats.updateColumnStatsState(deriveStatType(emptyStats, neededColumns));
      }
    }
    return stats;
  }

  private static void setUnknownRcDsToAverage(
      List<Long> rowCounts, List<Long> dataSizes, int avgRowSize) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Estimated average row size: " + avgRowSize);
    }
    for (int i = 0; i < rowCounts.size(); i++) {
      long rc = rowCounts.get(i);
      long s = dataSizes.get(i);
      if (rc <= 0 && s > 0) {
        rc = s / avgRowSize;
        rowCounts.set(i, rc);
      }

      if (s <= 0 && rc > 0) {
        s = rc * avgRowSize;
        dataSizes.set(i, s);
      }
    }
  }

  public static int estimateRowSizeFromSchema(HiveConf conf, List<ColumnInfo> schema,
      List<String> neededColumns) {
    int avgRowSize = 0;
    for (String neededCol : neededColumns) {
      ColumnInfo ci = getColumnInfoForColumn(neededCol, schema);
      ObjectInspector oi = ci.getObjectInspector();
      String colType = ci.getTypeName();
      if (colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)
          || colType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)
          || colType.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
          || colType.startsWith(serdeConstants.CHAR_TYPE_NAME)
          || colType.startsWith(serdeConstants.LIST_TYPE_NAME)
          || colType.startsWith(serdeConstants.MAP_TYPE_NAME)
          || colType.startsWith(serdeConstants.STRUCT_TYPE_NAME)
          || colType.startsWith(serdeConstants.UNION_TYPE_NAME)) {
        avgRowSize += getAvgColLenOfVariableLengthTypes(conf, oi, colType);
      } else {
        avgRowSize += getAvgColLenOfFixedLengthTypes(colType);
      }
    }
    return avgRowSize;
  }

  private static ColumnInfo getColumnInfoForColumn(String neededCol, List<ColumnInfo> schema) {
    for (ColumnInfo ci : schema) {
      if (ci.getInternalName().equalsIgnoreCase(neededCol)) {
        return ci;
      }
    }
    return null;
  }

  /**
   * Find the bytes on disk occupied by a table
   * @param conf
   *          - hive conf
   * @param table
   *          - table
   * @return size on disk
   */
  public static long getFileSizeForTable(HiveConf conf, Table table) {
    Path path = table.getPath();
    long size = 0;
    try {
      FileSystem fs = path.getFileSystem(conf);
      size = fs.getContentSummary(path).getLength();
    } catch (Exception e) {
      size = 0;
    }
    return size;
  }

  /**
   * Find the bytes on disks occupied by list of partitions
   * @param conf
   *          - hive conf
   * @param parts
   *          - partition list
   * @return sizes of patitions
   */
  public static List<Long> getFileSizeForPartitions(HiveConf conf, List<Partition> parts) {
    List<Long> sizes = Lists.newArrayList();
    for (Partition part : parts) {
      Path path = part.getDataLocation();
      long size = 0;
      try {
        FileSystem fs = path.getFileSystem(conf);
        size = fs.getContentSummary(path).getLength();
      } catch (Exception e) {
        size = 0;
      }
      sizes.add(size);
    }
    return sizes;
  }

  private static boolean containsNonPositives(List<Long> vals) {
    for (Long val : vals) {
      if (val <= 0L) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get sum of all values in the list that are >0
   * @param vals
   *          - list of values
   * @return sum
   */
  public static long getSumIgnoreNegatives(List<Long> vals) {
    long result = 0;
    for (Long l : vals) {
      if (l > 0) {
        result += l;
      }
    }
    return result;
  }

  private static Statistics.State deriveStatType(
      List<ColStatistics> colStats, List<String> neededColumns) {
    boolean hasStats = false,
        hasNull = (colStats == null) || (colStats.size() < neededColumns.size());
    if (colStats != null) {
      for (ColStatistics cs : colStats) {
        boolean isNull = cs == null;
        hasStats |= !isNull;
        hasNull |= isNull;
        if (hasNull && hasStats) break;
      }
    }
    State result = (hasStats
        ? (hasNull ? Statistics.State.PARTIAL : Statistics.State.COMPLETE)
        : (neededColumns.isEmpty() ? Statistics.State.COMPLETE : Statistics.State.NONE));
    return result;
  }

  /**
   * Convert ColumnStatisticsObj to ColStatistics
   * @param cso
   *          - ColumnStatisticsObj
   * @param tabName
   *          - table name
   * @param colName
   *          - column name
   * @return ColStatistics
   */
  public static ColStatistics getColStatistics(ColumnStatisticsObj cso, String tabName,
      String colName) {
    ColStatistics cs = new ColStatistics(tabName, colName, cso.getColType());
    String colType = cso.getColType();
    ColumnStatisticsData csd = cso.getStatsData();
    if (colType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)) {
      cs.setCountDistint(csd.getLongStats().getNumDVs());
      cs.setNumNulls(csd.getLongStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive1());
    } else if (colType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
      cs.setCountDistint(csd.getLongStats().getNumDVs());
      cs.setNumNulls(csd.getLongStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive2());
    } else if (colType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
      cs.setCountDistint(csd.getDoubleStats().getNumDVs());
      cs.setNumNulls(csd.getDoubleStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive1());
    } else if (colType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
      cs.setCountDistint(csd.getDoubleStats().getNumDVs());
      cs.setNumNulls(csd.getDoubleStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive2());
    } else if (colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)
        || colType.startsWith(serdeConstants.CHAR_TYPE_NAME)
        || colType.startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {
      cs.setCountDistint(csd.getStringStats().getNumDVs());
      cs.setNumNulls(csd.getStringStats().getNumNulls());
      cs.setAvgColLen(csd.getStringStats().getAvgColLen());
    } else if (colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
      if (csd.getBooleanStats().getNumFalses() > 0 && csd.getBooleanStats().getNumTrues() > 0) {
        cs.setCountDistint(2);
      } else {
        cs.setCountDistint(1);
      }
      cs.setNumTrues(csd.getBooleanStats().getNumTrues());
      cs.setNumFalses(csd.getBooleanStats().getNumFalses());
      cs.setNumNulls(csd.getBooleanStats().getNumNulls());
      cs.setAvgColLen(JavaDataModel.get().primitive1());
    } else if (colType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)) {
      cs.setAvgColLen(csd.getBinaryStats().getAvgColLen());
      cs.setNumNulls(csd.getBinaryStats().getNumNulls());
    } else if (colType.equalsIgnoreCase(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfTimestamp());
    } else if (colType.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfDecimal());
    } else if (colType.equalsIgnoreCase(serdeConstants.DATE_TYPE_NAME)) {
      cs.setAvgColLen(JavaDataModel.get().lengthOfDate());
    } else {
      // Columns statistics for complex datatypes are not supported yet
      return null;
    }
    return cs;
  }

  /**
   * Get table level column statistics from metastore for needed columns
   * @param table
   *          - table
   * @param schema
   *          - output schema
   * @param neededColumns
   *          - list of needed columns
   * @return column statistics
   */
  public static List<ColStatistics> getTableColumnStats(
      Table table, List<ColumnInfo> schema, List<String> neededColumns) {
    String dbName = table.getDbName();
    String tabName = table.getTableName();
    Map<String, String> colToTabAlias = new HashMap<String, String>(schema.size());
    List<String> neededColsInTable = processNeededColumns(schema, neededColumns, colToTabAlias);
    List<ColStatistics> stats = null;
    try {
      List<ColumnStatisticsObj> colStat = Hive.get().getTableColumnStatistics(
          dbName, tabName, neededColsInTable);
      stats = new ArrayList<ColStatistics>(colStat.size());
      for (ColumnStatisticsObj statObj : colStat) {
        ColStatistics cs = getColStatistics(statObj, tabName, statObj.getColName());
        cs.setTableAlias(colToTabAlias.get(cs.getColumnName()));
        stats.add(cs);
      }
    } catch (HiveException e) {
      LOG.error("Failed to retrieve table statistics: ", e);
      stats = null;
    }
    return stats;
  }

  /**
   * Get table level column statistics from metastore for needed columns
   * @param table
   *          - table
   * @param schema
   *          - output schema
   * @param neededColumns
   *          - list of needed columns
   * @return column statistics
   */
  public static Map<String, List<ColStatistics>> getPartColumnStats(Table table,
      List<ColumnInfo> schema, List<String> partNames, List<String> neededColumns) {
    String dbName = table.getDbName();
    String tabName = table.getTableName();
    Map<String, String> colToTabAlias = new HashMap<String, String>(schema.size());
    List<String> neededColsInTable = processNeededColumns(schema, neededColumns, colToTabAlias);
    Map<String, List<ColStatistics>> stats = null;
    try {
      Map<String, List<ColumnStatisticsObj>> colStat = Hive.get().getPartitionColumnStatistics(
          dbName, tabName, partNames, neededColsInTable);
      stats = new HashMap<String, List<ColStatistics>>(colStat.size());
      for (Map.Entry<String, List<ColumnStatisticsObj>> entry : colStat.entrySet()) {
        List<ColStatistics> partStat = new ArrayList<ColStatistics>(entry.getValue().size());
        for (ColumnStatisticsObj statObj : entry.getValue()) {
          ColStatistics cs = getColStatistics(statObj, tabName, statObj.getColName());
          cs.setTableAlias(colToTabAlias.get(cs.getColumnName()));
          partStat.add(cs);
        }
        stats.put(entry.getKey(), partStat);
      }
    } catch (HiveException e) {
      LOG.error("Failed to retrieve partitions statistics: ", e);
      stats = null;
    }
    return stats;
  }

  private static List<String> processNeededColumns(List<ColumnInfo> schema,
      List<String> neededColumns, Map<String, String> colToTabAlias) {
    for (ColumnInfo col : schema) {
      if (col.isHiddenVirtualCol()) continue;
      colToTabAlias.put(col.getInternalName(), col.getTabAlias());
    }
    // Remove hidden virtual columns, as well as needed columns that are not
    // part of the table. TODO: the latter case should not really happen...
    List<String> neededColsInTable = null;
    int limit = neededColumns.size();
    for (int i = 0; i < limit; ++i) {
      if (colToTabAlias.containsKey(neededColumns.get(i))) continue;
      if (neededColsInTable == null) {
        neededColsInTable = Lists.newArrayList(neededColumns);
      }
      neededColsInTable.remove(i--);
      --limit;
    }
    return (neededColsInTable == null) ? neededColumns : neededColsInTable;
  }

  /**
   * Get the raw data size of variable length data types
   * @param conf
   *          - hive conf
   * @param oi
   *          - object inspector
   * @param colType
   *          - column type
   * @return raw data size
   */
  public static long getAvgColLenOfVariableLengthTypes(HiveConf conf, ObjectInspector oi,
      String colType) {

    long configVarLen = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAX_VARIABLE_LENGTH);

    if (colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)) {

      // constant string projection Ex: select "hello" from table
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        if (coi.getWritableConstantValue() == null) {
          return 0;
        }

        return coi.getWritableConstantValue().toString().length();
      } else if (oi instanceof WritableConstantStringObjectInspector) {

        // some UDFs return writable constant strings (fixed width)
        // Ex: select upper("hello") from table
        WritableConstantStringObjectInspector wcsoi = (WritableConstantStringObjectInspector) oi;

        return wcsoi.getWritableConstantValue().toString().length();
      } else if (oi instanceof WritableStringObjectInspector) {

        // some UDFs may emit strings of variable length. like pattern matching
        // UDFs. it's hard to find the length of such UDFs.
        // return the variable length from config
        return configVarLen;
      }
    } else if (colType.startsWith(serdeConstants.VARCHAR_TYPE_NAME)) {

      // constant varchar projection
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        if (coi.getWritableConstantValue() == null) {
          return 0;
        }

        return coi.getWritableConstantValue().toString().length();
      } else if (oi instanceof WritableConstantHiveVarcharObjectInspector) {

        WritableConstantHiveVarcharObjectInspector wcsoi =
            (WritableConstantHiveVarcharObjectInspector) oi;
        return wcsoi.getWritableConstantValue().toString().length();
      } else if (oi instanceof WritableHiveVarcharObjectInspector) {
        return ((WritableHiveVarcharObjectInspector) oi).getMaxLength();
      }
    } else if (colType.startsWith(serdeConstants.CHAR_TYPE_NAME)) {

      // constant char projection
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        if (coi.getWritableConstantValue() == null) {
          return 0;
        }

        return coi.getWritableConstantValue().toString().length();
      } else if (oi instanceof WritableConstantHiveCharObjectInspector) {

        WritableConstantHiveCharObjectInspector wcsoi =
            (WritableConstantHiveCharObjectInspector) oi;
        return wcsoi.getWritableConstantValue().toString().length();
      } else if (oi instanceof WritableHiveCharObjectInspector) {
        return ((WritableHiveCharObjectInspector) oi).getMaxLength();
      }
    } else if (colType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)) {

      // constant byte arrays
      if (oi instanceof ConstantObjectInspector) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;

        // if writable constant is null then return size 0
        if (coi.getWritableConstantValue() == null) {
          return 0;
        }

        BytesWritable bw = ((BytesWritable) coi.getWritableConstantValue());
        return bw.getLength();
      } else if (oi instanceof WritableConstantBinaryObjectInspector) {

        // writable constant byte arrays
        WritableConstantBinaryObjectInspector wcboi = (WritableConstantBinaryObjectInspector) oi;

        return wcboi.getWritableConstantValue().getLength();
      } else if (oi instanceof WritableBinaryObjectInspector) {

        // return the variable length from config
        return configVarLen;
      }
    } else {

      // complex types (map, list, struct, union)
      return getSizeOfComplexTypes(conf, oi);
    }

    return 0;
  }

  /**
   * Get the size of complex data types
   * @param conf
   *          - hive conf
   * @param oi
   *          - object inspector
   * @return raw data size
   */
  public static long getSizeOfComplexTypes(HiveConf conf, ObjectInspector oi) {
    long result = 0;
    int length = 0;
    int listEntries = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_LIST_NUM_ENTRIES);
    int mapEntries = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAP_NUM_ENTRIES);

    switch (oi.getCategory()) {
    case PRIMITIVE:
      String colType = oi.getTypeName();
      if (colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)
          || colType.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
          || colType.startsWith(serdeConstants.CHAR_TYPE_NAME)) {
        int avgColLen = (int) getAvgColLenOfVariableLengthTypes(conf, oi, colType);
        result += JavaDataModel.get().lengthForStringOfLength(avgColLen);
      } else if (colType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)) {
        int avgColLen = (int) getAvgColLenOfVariableLengthTypes(conf, oi, colType);
        result += JavaDataModel.get().lengthForByteArrayOfSize(avgColLen);
      } else {
        result += getAvgColLenOfFixedLengthTypes(colType);
      }
      break;
    case LIST:
      if (oi instanceof StandardConstantListObjectInspector) {

        // constant list projection of known length
        StandardConstantListObjectInspector scloi = (StandardConstantListObjectInspector) oi;
        length = scloi.getWritableConstantValue().size();

        // check if list elements are primitive or Objects
        ObjectInspector leoi = scloi.getListElementObjectInspector();
        if (leoi.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
          result += getSizeOfPrimitiveTypeArraysFromType(leoi.getTypeName(), length);
        } else {
          result += JavaDataModel.get().lengthForObjectArrayOfSize(length);
        }
      } else {
        StandardListObjectInspector sloi = (StandardListObjectInspector) oi;

        // list overhead + (configured number of element in list * size of element)
        long elemSize = getSizeOfComplexTypes(conf, sloi.getListElementObjectInspector());
        result += JavaDataModel.get().arrayList() + (listEntries * elemSize);
      }
      break;
    case MAP:
      if (oi instanceof StandardConstantMapObjectInspector) {

        // constant map projection of known length
        StandardConstantMapObjectInspector scmoi = (StandardConstantMapObjectInspector) oi;
        result += getSizeOfMap(scmoi);
      } else {
        StandardMapObjectInspector smoi = (StandardMapObjectInspector) oi;
        result += getSizeOfComplexTypes(conf, smoi.getMapKeyObjectInspector());
        result += getSizeOfComplexTypes(conf, smoi.getMapValueObjectInspector());

        // hash map overhead
        result += JavaDataModel.get().hashMap(mapEntries);
      }
      break;
    case STRUCT:
      StructObjectInspector soi = (StructObjectInspector) oi;

      // add constant object overhead for struct
      result += JavaDataModel.get().object();

      // add constant struct field names references overhead
      result += soi.getAllStructFieldRefs().size() * JavaDataModel.get().ref();
      for (StructField field : soi.getAllStructFieldRefs()) {
        result += getSizeOfComplexTypes(conf, field.getFieldObjectInspector());
      }
      break;
    case UNION:
      UnionObjectInspector uoi = (UnionObjectInspector) oi;

      // add constant object overhead for union
      result += JavaDataModel.get().object();

      // add constant size for unions tags
      result += uoi.getObjectInspectors().size() * JavaDataModel.get().primitive1();
      for (ObjectInspector foi : uoi.getObjectInspectors()) {
        result += getSizeOfComplexTypes(conf, foi);
      }
      break;
    default:
      break;
    }

    return result;
  }

  /**
   * Get size of fixed length primitives
   * @param colType
   *          - column type
   * @return raw data size
   */
  public static long getAvgColLenOfFixedLengthTypes(String colType) {
    if (colType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
      return JavaDataModel.get().primitive1();
    } else if (colType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
      return JavaDataModel.get().primitive2();
    } else if (colType.equalsIgnoreCase(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      return JavaDataModel.get().lengthOfTimestamp();
    } else if (colType.equalsIgnoreCase(serdeConstants.DATE_TYPE_NAME)) {
      return JavaDataModel.get().lengthOfDate();
    } else if (colType.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      return JavaDataModel.get().lengthOfDecimal();
    } else {
      return 0;
    }
  }

  /**
   * Get the size of arrays of primitive types
   * @param colType
   *          - column type
   * @param length
   *          - array length
   * @return raw data size
   */
  public static long getSizeOfPrimitiveTypeArraysFromType(String colType, int length) {
    if (colType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)) {
      return JavaDataModel.get().lengthForIntArrayOfSize(length);
    } else if (colType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {
      return JavaDataModel.get().lengthForDoubleArrayOfSize(length);
    } else if (colType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)) {
      return JavaDataModel.get().lengthForLongArrayOfSize(length);
    } else if (colType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)) {
      return JavaDataModel.get().lengthForByteArrayOfSize(length);
    } else if (colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
      return JavaDataModel.get().lengthForBooleanArrayOfSize(length);
    } else if (colType.equalsIgnoreCase(serdeConstants.TIMESTAMP_TYPE_NAME)) {
      return JavaDataModel.get().lengthForTimestampArrayOfSize(length);
    } else if (colType.equalsIgnoreCase(serdeConstants.DATE_TYPE_NAME)) {
      return JavaDataModel.get().lengthForDateArrayOfSize(length);
    } else if (colType.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
      return JavaDataModel.get().lengthForDecimalArrayOfSize(length);
    } else {
      return 0;
    }
  }

  /**
   * Estimate the size of map object
   * @param scmoi
   *          - object inspector
   * @return size of map
   */
  public static long getSizeOfMap(StandardConstantMapObjectInspector scmoi) {
    Map<?, ?> map = scmoi.getWritableConstantValue();
    ObjectInspector koi = scmoi.getMapKeyObjectInspector();
    ObjectInspector voi = scmoi.getMapValueObjectInspector();
    long result = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      result += getWritableSize(koi, entry.getKey());
      result += getWritableSize(voi, entry.getValue());
    }

    // add additional overhead of each map entries
    result += JavaDataModel.get().hashMap(map.entrySet().size());
    return result;
  }

  /**
   * Get size of primitive data types based on their respective writable object inspector
   * @param oi
   *          - object inspector
   * @param value
   *          - value
   * @return raw data size
   */
  public static long getWritableSize(ObjectInspector oi, Object value) {
    if (oi instanceof WritableStringObjectInspector) {
      WritableStringObjectInspector woi = (WritableStringObjectInspector) oi;
      return JavaDataModel.get().lengthForStringOfLength(
          woi.getPrimitiveWritableObject(value).getLength());
    } else if (oi instanceof WritableBinaryObjectInspector) {
      WritableBinaryObjectInspector woi = (WritableBinaryObjectInspector) oi;
      return JavaDataModel.get().lengthForByteArrayOfSize(
          woi.getPrimitiveWritableObject(value).getLength());
    } else if (oi instanceof WritableBooleanObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableByteObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableDateObjectInspector) {
      return JavaDataModel.get().lengthOfDate();
    } else if (oi instanceof WritableDoubleObjectInspector) {
      return JavaDataModel.get().primitive2();
    } else if (oi instanceof WritableFloatObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableHiveDecimalObjectInspector) {
      return JavaDataModel.get().lengthOfDecimal();
    } else if (oi instanceof WritableIntObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableLongObjectInspector) {
      return JavaDataModel.get().primitive2();
    } else if (oi instanceof WritableShortObjectInspector) {
      return JavaDataModel.get().primitive1();
    } else if (oi instanceof WritableTimestampObjectInspector) {
      return JavaDataModel.get().lengthOfTimestamp();
    }

    return 0;
  }

  /**
   * Get column statistics from parent statistics.
   * @param conf
   *          - hive conf
   * @param parentStats
   *          - parent statistics
   * @param colExprMap
   *          - column expression map
   * @param rowSchema
   *          - row schema
   * @return column statistics
   */
  public static List<ColStatistics> getColStatisticsFromExprMap(HiveConf conf,
      Statistics parentStats, Map<String, ExprNodeDesc> colExprMap, RowSchema rowSchema) {

    List<ColStatistics> cs = Lists.newArrayList();
    if (colExprMap != null) {
      for (ColumnInfo ci : rowSchema.getSignature()) {
        String outColName = ci.getInternalName();
        String outTabAlias = ci.getTabAlias();
        ExprNodeDesc end = colExprMap.get(outColName);
        if (end == null) {
          outColName = StatsUtils.stripPrefixFromColumnName(outColName);
          end = colExprMap.get(outColName);
        }
        ColStatistics colStat = getColStatisticsFromExpression(conf, parentStats, end);
        if (colStat != null) {
          outColName = StatsUtils.stripPrefixFromColumnName(outColName);
          colStat.setColumnName(outColName);
          colStat.setTableAlias(outTabAlias);
        }
        cs.add(colStat);
      }
    }
    return cs;
  }

  /**
   * Get column statistics expression nodes
   * @param conf
   *          - hive conf
   * @param parentStats
   *          - parent statistics
   * @param end
   *          - expression nodes
   * @return column statistics
   */
  public static ColStatistics getColStatisticsFromExpression(HiveConf conf, Statistics parentStats,
      ExprNodeDesc end) {

    if (end == null) {
      return null;
    }

    String colName = null;
    String colType = null;
    double avgColSize = 0;
    long countDistincts = 0;
    long numNulls = 0;
    ObjectInspector oi = null;
    long numRows = parentStats.getNumRows();
    String tabAlias = null;

    if (end instanceof ExprNodeColumnDesc) {
      // column projection
      ExprNodeColumnDesc encd = (ExprNodeColumnDesc) end;
      colName = encd.getColumn();
      tabAlias = encd.getTabAlias();
      colName = stripPrefixFromColumnName(colName);

      if (encd.getIsPartitionColOrVirtualCol()) {

        // vitual columns
        colType = encd.getTypeInfo().getTypeName();
        countDistincts = numRows;
        oi = encd.getWritableObjectInspector();
      } else {

        // clone the column stats and return
        ColStatistics result = parentStats.getColumnStatisticsForColumn(tabAlias, colName);
        if (result != null) {
          try {
            return result.clone();
          } catch (CloneNotSupportedException e) {
            return null;
          }
        }
        return null;
      }
    } else if (end instanceof ExprNodeConstantDesc) {

      // constant projection
      ExprNodeConstantDesc encd = (ExprNodeConstantDesc) end;

      // null projection
      if (encd.getValue() == null) {
        colName = encd.getName();
        colType = "null";
        numNulls = numRows;
      } else {
        colName = encd.getName();
        colType = encd.getTypeString();
        countDistincts = 1;
        oi = encd.getWritableObjectInspector();
      }
    } else if (end instanceof ExprNodeGenericFuncDesc) {

      // udf projection
      ExprNodeGenericFuncDesc engfd = (ExprNodeGenericFuncDesc) end;
      colName = engfd.getName();
      colType = engfd.getTypeString();
      countDistincts = numRows;
      oi = engfd.getWritableObjectInspector();
    } else if (end instanceof ExprNodeNullDesc) {

      // null projection
      ExprNodeNullDesc ennd = (ExprNodeNullDesc) end;
      colName = ennd.getName();
      colType = "null";
      numNulls = numRows;
    }

    if (colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)
        || colType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)
        || colType.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
        || colType.startsWith(serdeConstants.CHAR_TYPE_NAME)
        || colType.startsWith(serdeConstants.LIST_TYPE_NAME)
        || colType.startsWith(serdeConstants.MAP_TYPE_NAME)
        || colType.startsWith(serdeConstants.STRUCT_TYPE_NAME)
        || colType.startsWith(serdeConstants.UNION_TYPE_NAME)) {
      avgColSize = getAvgColLenOfVariableLengthTypes(conf, oi, colType);
    } else {
      avgColSize = getAvgColLenOfFixedLengthTypes(colType);
    }

    ColStatistics colStats = new ColStatistics(tabAlias, colName, colType);
    colStats.setAvgColLen(avgColSize);
    colStats.setCountDistint(countDistincts);
    colStats.setNumNulls(numNulls);

    return colStats;
  }

  /**
   * Get number of rows of a give table
   * @return number of rows
   */
  public static long getNumRows(Table table) {
    return getBasicStatForTable(table, StatsSetupConst.ROW_COUNT);
  }

  /**
   * Get raw data size of a give table
   * @return raw data size
   */
  public static long getRawDataSize(Table table) {
    return getBasicStatForTable(table, StatsSetupConst.RAW_DATA_SIZE);
  }

  /**
   * Get total size of a give table
   * @return total size
   */
  public static long getTotalSize(Table table) {
    return getBasicStatForTable(table, StatsSetupConst.TOTAL_SIZE);
  }

  /**
   * Get basic stats of table
   * @param dbName
   *          - database name
   * @param tabName
   *          - table name
   * @param statType
   *          - type of stats
   * @return value of stats
   */
  public static long getBasicStatForTable(Table table, String statType) {
    Map<String, String> params = table.getParameters();
    long result = 0;

    if (params != null) {
      try {
        result = Long.parseLong(params.get(statType));
      } catch (NumberFormatException e) {
        result = 0;
      }
    }
    return result;
  }

  /**
   * Get basic stats of partitions
   * @param table
   *          - table
   * @param parts
   *          - partitions
   * @param statType
   *          - type of stats
   * @return value of stats
   */
  public static List<Long> getBasicStatForPartitions(Table table, List<Partition> parts,
      String statType) {

    List<Long> stats = Lists.newArrayList();
    for (Partition part : parts) {
      Map<String, String> params = part.getParameters();
      long result = 0;
      if (params != null) {
        try {
          result = Long.parseLong(params.get(statType));
        } catch (NumberFormatException e) {
          result = 0;
        }
        stats.add(result);
      }
    }
    return stats;
  }

  /**
   * Compute raw data size from column statistics
   * @param numRows
   *          - number of rows
   * @param colStats
   *          - column statistics
   * @return raw data size
   */
  public static long getDataSizeFromColumnStats(long numRows, List<ColStatistics> colStats) {
    long result = 0;

    if (numRows <= 0) {
      return result;
    }

    for (ColStatistics cs : colStats) {
      if (cs != null) {
        String colType = cs.getColumnType();
        long nonNullCount = numRows - cs.getNumNulls();
        if (colType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME)
            || colType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME)
            || colType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME)
            || colType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME)
            || colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)
            || colType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME)
            || colType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME)) {

          result += nonNullCount * cs.getAvgColLen();
        } else if (colType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME)
            || colType.startsWith(serdeConstants.VARCHAR_TYPE_NAME)
            || colType.startsWith(serdeConstants.CHAR_TYPE_NAME)) {

          int acl = (int) Math.round(cs.getAvgColLen());
          result += nonNullCount * JavaDataModel.get().lengthForStringOfLength(acl);
        } else if (colType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME)) {

          int acl = (int) Math.round(cs.getAvgColLen());
          result += nonNullCount * JavaDataModel.get().lengthForByteArrayOfSize(acl);
        } else if (colType.equalsIgnoreCase(serdeConstants.TIMESTAMP_TYPE_NAME)) {

          result += nonNullCount * JavaDataModel.get().lengthOfTimestamp();
        } else if (colType.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {

          result += nonNullCount * JavaDataModel.get().lengthOfDecimal();
        } else if (colType.equalsIgnoreCase(serdeConstants.DATE_TYPE_NAME)) {

          result += nonNullCount * JavaDataModel.get().lengthOfDate();
        } else {

          result += nonNullCount * cs.getAvgColLen();
        }
      }
    }

    return result;
  }

  /**
   * Remove KEY/VALUE prefix from column name
   * @param colName
   *          - column name
   * @return column name
   */
  public static String stripPrefixFromColumnName(String colName) {
    String stripedName = colName;
    if (colName.startsWith("KEY._") || colName.startsWith("VALUE._")) {
      // strip off KEY./VALUE. from column name
      stripedName = colName.split("\\.")[1];
    }
    return stripedName;
  }

  /**
   * Returns fully qualified name of column
   * @param tabName
   * @param colName
   * @return
   */
  public static String getFullyQualifiedColumnName(String tabName, String colName) {
    return getFullyQualifiedName(null, tabName, colName);
  }

  /**
   * Returns fully qualified name of column
   * @param dbName
   * @param tabName
   * @param colName
   * @return
   */
  public static String getFullyQualifiedColumnName(String dbName, String tabName, String colName) {
    return getFullyQualifiedName(dbName, tabName, colName);
  }

  /**
   * Returns fully qualified name of column
   * @param dbName
   * @param tabName
   * @param partName
   * @param colName
   * @return
   */
  public static String getFullyQualifiedColumnName(String dbName, String tabName, String partName,
      String colName) {
    return getFullyQualifiedName(dbName, tabName, partName, colName);
  }

  private static String getFullyQualifiedName(String... names) {
    List<String> nonNullAndEmptyNames = Lists.newArrayList();
    for (String name : names) {
      if (name != null && !name.isEmpty()) {
        nonNullAndEmptyNames.add(name);
      }
    }
    return Joiner.on(".").join(nonNullAndEmptyNames);
  }

  /**
   * Try to get fully qualified column name from expression node
   * @param keyExprs
   *          - expression nodes
   * @param map
   *          - column expression map
   * @return list of fully qualified names
   */
  public static List<String> getFullQualifedColNameFromExprs(List<ExprNodeDesc> keyExprs,
      Map<String, ExprNodeDesc> map) {
    List<String> result = Lists.newArrayList();
    if (keyExprs != null) {
      for (ExprNodeDesc end : keyExprs) {
        String outColName = null;
        for (Map.Entry<String, ExprNodeDesc> entry : map.entrySet()) {
          if (entry.getValue().isSame(end)) {
            outColName = entry.getKey();
          }
        }
        if (end instanceof ExprNodeColumnDesc) {
          ExprNodeColumnDesc encd = (ExprNodeColumnDesc) end;
          if (outColName == null) {
            outColName = encd.getColumn();
          }
          String tabAlias = encd.getTabAlias();
          outColName = stripPrefixFromColumnName(outColName);
          result.add(getFullyQualifiedColumnName(tabAlias, outColName));
        } else if (end instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc enf = (ExprNodeGenericFuncDesc) end;
          List<String> cols = getFullQualifedColNameFromExprs(enf.getChildren(), map);
          String joinedStr = Joiner.on(".").skipNulls().join(cols);
          result.add(joinedStr);
        } else if (end instanceof ExprNodeConstantDesc) {
          ExprNodeConstantDesc encd = (ExprNodeConstantDesc) end;
          result.add(encd.getValue().toString());
        }
      }
    }
    return result;
  }
}
