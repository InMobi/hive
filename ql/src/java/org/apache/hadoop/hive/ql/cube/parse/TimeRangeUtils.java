package org.apache.hadoop.hive.ql.cube.parse;

public class TimeRangeUtils {
  public static String getTimeRangePartitionFilter(
    FactPartition partition,
    CubeQueryContext cubeQueryContext,
    String tableName
  ) {
    String partCol = partition.getPartCol();
    String partFilter;

    if (cubeQueryContext  != null && !cubeQueryContext.shouldReplaceTimeDimWithPart()) {
      String replacedPartCol = cubeQueryContext.getTimeDimOfPartitionColumn(partCol);
      if (!partCol.equalsIgnoreCase(replacedPartCol)) {
        partFilter = partition.getFormattedFilter(replacedPartCol, tableName);
      } else {
        partFilter = partition.getFormattedFilter(tableName);
      }
    } else {
      partFilter = partition.getFormattedFilter(tableName);
    }

    return partFilter;
  }
}
