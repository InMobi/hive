package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class MetastoreUtil implements MetastoreConstants {

  public static final String getFactStorageTableName(String factName,
      String storageName) {
    return getStorageTableName(factName, Storage.getPrefix(storageName));
  }

  public static final String getDimStorageTableName(String dimName,
      String storageName) {
    return getStorageTableName(dimName, Storage.getPrefix(storageName));
  }

  public static final String getStorageTableName(String cubeTableName,
      String storagePrefix) {
    return storagePrefix + cubeTableName;
  }

  public static String getStorageClassKey(String name) {
    return getStorageEntityPrefix(name) + CLASS_SFX;
  }

  public static final String getStorageEntityPrefix(String storageName) {
    return STORAGE_ENTITY_PFX + storageName.toLowerCase();
  }

  // ///////////////////////
  // Dimension properties//
  // ///////////////////////
  public static final String getDimTypePropertyKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + TYPE_SFX;
  }

  public static String getHierachyElementKeyPFX(String dimName) {
    return getDimensionKeyPrefix(dimName) + HIERARCHY_SFX;
  }

  public static String getHierachyElementKeyName(String dimName, int index) {
    return getHierachyElementKeyPFX(dimName) + index;
  }

  public static Integer getHierachyElementIndex(String dimName, String param) {
    return Integer.parseInt(param.substring(getHierachyElementKeyPFX(
        dimName).length()));
  }

  public static final String getDimensionSrcReferenceKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + DIM_REFERS_SFX;
  }

  public static final String getDimensionDestReference(String tableName,
      String columnName) {
    return tableName.toLowerCase() + TABLE_COLUMN_SEPERATOR
        + columnName.toLowerCase();
  }

  public static final String getDimensionDestReference(
      List<TableReference> references) {
    String toks[] = new String[references.size()];

    for (int i = 0; i < references.size(); i++) {
      TableReference reference = references.get(i);
      toks[i] = reference.getDestTable() + TABLE_COLUMN_SEPERATOR
          + reference.getDestColumn();
    }

    return StringUtils.join(toks, ',');
  }

  //////////////////////////
  // Column properties //
  //////////////////////////
  public static String getColumnKeyPrefix(String colName) {
    return COL_PFX + colName.toLowerCase();
  }

  public static String getCubeColStartTimePropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + START_TIME_SFX;
  }

  public static String getCubeColEndTimePropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + END_TIME_SFX;
  }

  public static String getCubeColCostPropertyKey(String colName) {
    return getColumnKeyPrefix(colName) + COST_SFX;
  }

  //////////////////////////
  // Dimension properties //
  //////////////////////////
  public static String getInlineDimensionSizeKey(String name) {
    return getDimensionKeyPrefix(name) + INLINE_SIZE_SFX;
  }

  public static String getInlineDimensionValuesKey(String name) {
    return getDimensionKeyPrefix(name) + INLINE_VALUES_SFX;
  }

  public static String getDimensionKeyPrefix(String dimName) {
    return DIM_KEY_PFX + dimName.toLowerCase();
  }

  public static String getDimensionDumpPeriodKey(String name, String storage) {
    return getDimensionKeyPrefix(name) + "." + storage.toLowerCase() +
        DUMP_PERIOD_SFX;
  }

  public static String getDimensionStorageListKey(String name) {
    return getDimensionKeyPrefix(name) + STORAGE_LIST_SFX;
  }

  public static final String getDimensionClassPropertyKey(String dimName) {
    return getDimensionKeyPrefix(dimName) + CLASS_SFX;
  }

  // //////////////////////////
  // Measure properties ///
  // /////////////////////////
  public static final String getMeasurePrefix(String measureName) {
    return MEASURE_KEY_PFX + measureName.toLowerCase();
  }

  public static final String getMeasureClassPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + CLASS_SFX;
  }

  public static final String getMeasureUnitPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + UNIT_SFX;
  }

  public static final String getMeasureTypePropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + TYPE_SFX;
  }

  public static final String getMeasureFormatPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + FORMATSTRING_SFX;
  }

  public static final String getMeasureAggrPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + AGGR_SFX;
  }

  public static final String getMeasureExprPropertyKey(String measureName) {
    return getMeasurePrefix(measureName) + EXPR_SFX;
  }

  // //////////////////////////
  // Cube properties ///
  // /////////////////////////
  public static final String getCubePrefix(String cubeName) {
    return CUBE_KEY_PFX + cubeName.toLowerCase();
  }

  public static final String getCubeMeasureListKey(String cubeName) {
    return getCubePrefix(cubeName) + MEASURES_LIST_SFX;
  }

  public static final String getCubeDimensionListKey(String cubeName) {
    return getCubePrefix(cubeName) + DIMENSIONS_LIST_SFX;
  }

  public static final String getCubeTimedDimensionListKey(String cubeName) {
    return getCubePrefix(cubeName) + TIMED_DIMENSIONS_LIST_SFX;
  }

  public static final String getCubeTableKeyPrefix(String tableName) {
    return CUBE_TABLE_PFX + tableName.toLowerCase();
  }

  // //////////////////////////
  // Fact propertes ///
  // /////////////////////////
  public static String getFactStorageListKey(String name) {
    return getFactKeyPrefix(name) + STORAGE_LIST_SFX;
  }

  public static String getFactKeyPrefix(String factName) {
    return FACT_KEY_PFX + factName.toLowerCase();
  }

  public static String getFactUpdatePeriodKey(String name, String storage) {
    return getFactKeyPrefix(name) + "." + storage.toLowerCase()
        + UPDATE_PERIOD_SFX;
  }

  public static String getFactCubeNamesKey(String name) {
    return getFactKeyPrefix(name) + CUBE_NAMES_SFX;
  }

  public static String getValidColumnsKey(String name) {
    return getFactKeyPrefix(name) + VALID_COLUMNS_SFX;
  }

  public static String getCubeTableWeightKey(String name) {
    return getCubeTableKeyPrefix(name) + WEIGHT_KEY_SFX;
  }

  public static String getLatestPartTimestampKey(String partCol) {
    return MetastoreConstants.STORAGE_PFX + partCol +
        MetastoreConstants.LATEST_PART_TIMESTAMP_SFX;
  }

  // //////////////////////////
  // Utils ///
  // /////////////////////////
  public static <E extends Named> String getNamedStr(Collection<E> set) {
    if (set == null || set.isEmpty()) {
      return "";
    }
    StringBuilder valueStr = new StringBuilder();
    Iterator<E> it = set.iterator();
    for (int i = 0; i < (set.size() - 1); i++) {
      valueStr.append(it.next().getName());
      valueStr.append(",");
    }
    valueStr.append(it.next().getName());
    return valueStr.toString();
  }

  public static String getObjectStr(Collection<?> set) {
    if (set == null || set.isEmpty()) {
      return "";
    }
    StringBuilder valueStr = new StringBuilder();
    Iterator<?> it = set.iterator();
    for (int i = 0; i < (set.size() - 1); i++) {
      valueStr.append(it.next().toString());
      valueStr.append(",");
    }
    valueStr.append(it.next().toString());
    return valueStr.toString();
  }

  public static String getStr(Collection<String> set) {
    if (set == null || set.isEmpty()) {
      return "";
    }
    StringBuilder valueStr = new StringBuilder();
    Iterator<String> it = set.iterator();
    for (int i = 0; i < (set.size() - 1); i++) {
      valueStr.append(it.next());
      valueStr.append(",");
    }
    valueStr.append(it.next());
    return valueStr.toString();
  }

  public static List<String> getColumnNames(AbstractCubeTable table) {
    List<FieldSchema> fields = table.getColumns();
    List<String> columns = new ArrayList<String>(fields.size());
    for (FieldSchema f : fields) {
      columns.add(f.getName().toLowerCase());
    }
    return columns;
  }

  public static List<String> getCubeMeasureNames(Cube table) {
    List<String> columns = new ArrayList<String>();
    for (CubeMeasure f : table.getMeasures()) {
      columns.add(f.getName().toLowerCase());
    }
    return columns;
  }

  public static List<String> getCubeDimensionNames(Cube table) {
    List<String> columns = new ArrayList<String>();
    for (CubeDimension f : table.getDimensions()) {
      addColumnNames(f, columns);
    }
    return columns;
  }

  private static void addColumnNames(CubeDimension dim, List<String> cols) {
    if (dim instanceof HierarchicalDimension) {
      HierarchicalDimension h = (HierarchicalDimension) dim;
      for (CubeDimension d : h.getHierarchy()) {
        addColumnNames(d, cols);
      }
    } else {
      cols.add(dim.getName().toLowerCase());
    }
  }
}
