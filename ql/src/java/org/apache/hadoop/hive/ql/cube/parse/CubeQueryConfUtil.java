package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class CubeQueryConfUtil {
  public static final String STORAGE_TABLES_SFX = ".storagetables";
  public static final String UPDATE_PERIODS_SFX = ".updateperiods";
  public static final String FACT_TABLES_SFX = ".facttables";
  public static final String STORAGE_KEY_PFX = ".storage.";
  public static final String VALID_PFX = "cube.query.valid.";
  public static final String VALID_FACT_PFX = "cube.query.valid." +
      "fact.";

  public static final String VALID_STORAGE_DIM_TABLES = "cube.query.valid." +
      "dim.storgaetables";
  public static final String DRIVER_SUPPORTED_STORAGES = "cube.query.driver." +
      "supported.storages";
  public static final String FAIL_QUERY_ON_PARTIAL_DATA =
      "cube.query.fail.if.data.partial";
  public static final String NON_EXISTING_PARTITIONS =
      "cube.query.nonexisting.partitions";
  public static final String ENABLE_MULTI_TABLE_SELECT =
      "cube.query.enable.multi.table.select";

  public static final boolean DEFAULT_MULTI_TABLE_SELECT = true;

  private static String getValidKeyCubePFX(String cubeName) {
    return VALID_PFX + cubeName.toLowerCase();
  }

  private static String getValidKeyFactPFX(String factName) {
    return VALID_FACT_PFX + factName.toLowerCase();
  }

  private static String getValidKeyStoragePFX(String factName,
      String storage) {
    return getValidKeyFactPFX(factName) + STORAGE_KEY_PFX + storage.toLowerCase();
  }

  public static String getValidFactTablesKey(String cubeName) {
    return getValidKeyCubePFX(cubeName) + FACT_TABLES_SFX;
  }

  public static String getValidStorageTablesKey(String factName) {
    return getValidKeyFactPFX(factName) + STORAGE_TABLES_SFX;
  }

  public static String getValidUpdatePeriodsKey(String fact,
      String storage) {
    return getValidKeyStoragePFX(fact, storage) + UPDATE_PERIODS_SFX;
  }

  public static List<String> getStringList(Configuration conf, String keyName) {
    String str = conf.get(keyName);
    List<String> list = StringUtils.isBlank(str) ? null :
      Arrays.asList(StringUtils.split(str.toLowerCase(), ","));
    return list;
  }
}
