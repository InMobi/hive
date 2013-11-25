package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StorageConstants {
  public static final String DATE_PARTITION_KEY = "dt";
  public static final String STORGAE_SEPARATOR = "_";
  public static final String LATEST_PARTITION_VALUE = "latest";
  /**
   * Get the partition spec for latest partition
   *
   * @param The partition column for latest spec
   *
   * @return latest partition spec as Map from String to String
   */
  public static String getLatestPartFilter(String partCol) {
    return partCol + "='" + LATEST_PARTITION_VALUE + "'";
  }
  /**
   * Get the latest partition value as List
   *
   * @return List
   */
  public static List<String> getPartitionsForLatest() {
    List<String> parts = new ArrayList<String>();
    parts.add(LATEST_PARTITION_VALUE);
    return parts;
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
        LATEST_PARTITION_VALUE);
    return latestSpec;
  }
}
