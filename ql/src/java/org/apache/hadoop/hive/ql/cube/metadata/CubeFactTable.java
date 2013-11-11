package org.apache.hadoop.hive.ql.cube.metadata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod.UpdatePeriodComparator;
import org.apache.hadoop.hive.ql.cube.parse.DateUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public final class CubeFactTable extends AbstractCubeTable {
  private final List<String> cubeNames;
  private final Map<String, Set<UpdatePeriod>> storageUpdatePeriods;

  public CubeFactTable(Table hiveTable) {
    super(hiveTable);
    this.storageUpdatePeriods = getUpdatePeriods(getName(), getProperties());
    this.cubeNames = getCubeNames(getName(), getProperties());
  }

  public CubeFactTable(List<String> cubeNames, String factName,
      List<FieldSchema> columns,
      Map<String, Set<UpdatePeriod>> storageUpdatePeriods) {
    this(cubeNames, factName, columns, storageUpdatePeriods, 0L,
        new HashMap<String, String>());
  }

  public CubeFactTable(List<String> cubeNames, String factName,
      List<FieldSchema> columns,
      Map<String, Set<UpdatePeriod>> storageUpdatePeriods, double weight) {
    this(cubeNames, factName, columns, storageUpdatePeriods, weight,
        new HashMap<String, String>());
  }

  public CubeFactTable(List<String> cubeNames, String factName,
      List<FieldSchema> columns,
      Map<String, Set<UpdatePeriod>> storageUpdatePeriods,
      double weight,
      Map<String, String> properties) {
    super(factName, columns, properties, weight);
    this.cubeNames = cubeNames;
    this.storageUpdatePeriods = storageUpdatePeriods;
    addProperties();
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    addCubeNames(getName(), getProperties(), cubeNames);
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  private static void addUpdatePeriodProperies(String name,
      Map<String, String> props,
      Map<String, Set<UpdatePeriod>> updatePeriods) {
    if (updatePeriods != null) {
      props.put(MetastoreUtil.getFactStorageListKey(name),
          MetastoreUtil.getStr(updatePeriods.keySet()));
      for (Map.Entry<String, Set<UpdatePeriod>> entry : updatePeriods.entrySet()) {
        props.put(MetastoreUtil.getFactUpdatePeriodKey(name, entry.getKey()),
            MetastoreUtil.getNamedStr(entry.getValue()));
      }
    }
  }

  private static void addCubeNames(String factName, Map<String, String> props,
      List<String> cubeNames) {
    props.put(MetastoreUtil.getFactCubeNamesKey(factName),
        StringUtils.join(cubeNames, ",").toLowerCase());
  }

  private static Map<String, Set<UpdatePeriod>> getUpdatePeriods(String name,
      Map<String, String> props) {
    Map<String, Set<UpdatePeriod>> storageUpdatePeriods = new HashMap<String,
        Set<UpdatePeriod>>();
    String storagesStr = props.get(MetastoreUtil.getFactStorageListKey(name));
    String[] storages = storagesStr.split(",");
    for (String storage : storages) {
      String updatePeriodStr = props.get(MetastoreUtil.getFactUpdatePeriodKey(
          name, storage));
      String[] periods = updatePeriodStr.split(",");
      Set<UpdatePeriod> updatePeriods = new TreeSet<UpdatePeriod>();
      for (String period : periods) {
        updatePeriods.add(UpdatePeriod.valueOf(period));
      }
      storageUpdatePeriods.put(storage, updatePeriods);
    }
    return storageUpdatePeriods;
  }

  static List<String> getCubeNames(String factName, Map<String, String> props) {
    List<String> cubeNames = new ArrayList<String>();
    cubeNames.addAll(Arrays.asList(StringUtils.split(
        props.get(MetastoreUtil.getFactCubeNamesKey(factName)), ',')));
    return cubeNames;
  }

  public Map<String, Set<UpdatePeriod>> getUpdatePeriods() {
    return storageUpdatePeriods;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }

    CubeFactTable other = (CubeFactTable) obj;
    if (this.getUpdatePeriods() == null) {
      if (other.getUpdatePeriods() != null) {
        return false;
      }
    } else {
      if (!this.getUpdatePeriods().equals(other.getUpdatePeriods())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.FACT;
  }

  /**
   * Get partition value strings for given range, for the specified updateInterval
   *
   * @param fromDate
   * @param toDate
   * @param interval
   * @return
   */
  public List<String> getPartitions(Date fromDate, Date toDate,
      UpdatePeriod interval) {
    String fmt = interval.format();
    if (fmt != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTime(fromDate);
      List<String> partitions = new ArrayList<String>();
      Date dt = cal.getTime();
      while (dt.compareTo(toDate) < 0) {
        String part = new SimpleDateFormat(fmt).format(cal.getTime());
        partitions.add(part);
        cal.add(interval.calendarField(), 1);
        dt = cal.getTime();
      }
      return partitions;
    } else {
      return null;
    }
  }

  /**
   * Get the max update period for the given range and available update periods
   *
   * @param from
   * @param to
   * @param updatePeriods
   * @return
   */
  public static UpdatePeriod maxIntervalInRange(Date from, Date to,
      Set<UpdatePeriod> updatePeriods) {
    UpdatePeriod max = null;

    long diff = to.getTime() - from.getTime();
    if (diff < UpdatePeriod.MIN_INTERVAL) {
      return null;
    }

    // Use weight only till UpdatePeriod.DAILY
    // Above Daily, check if at least one full update period is present
    // between the dates
    UpdatePeriodComparator cmp = new UpdatePeriodComparator();
    for (UpdatePeriod i : updatePeriods) {
      if (UpdatePeriod.YEARLY == i || UpdatePeriod.QUARTERLY == i
          || UpdatePeriod.MONTHLY == i) {
        int intervals = 0;
        switch (i) {
        case YEARLY:
          intervals = DateUtil.getYearsBetween(from, to);
          break;
        case QUARTERLY:
          intervals = DateUtil.getQuartersBetween(from, to);
          break;
        case MONTHLY:
          intervals = DateUtil.getMonthsBetween(from, to);
          break;
        case WEEKLY:
          intervals = DateUtil.getWeeksBetween(from, to);
          break;
        }

        if (intervals > 0) {
          if (cmp.compare(i, max) > 0) {
            max = i;
          }
        }
      } else {
        // Below WEEKLY, we can use weight to find out the correct period
        if (diff < i.weight()) {
          // interval larger than time diff
          continue;
        }

        if (cmp.compare(i, max) > 0) {
          max = i;
        }
      }
    }
    return max;
  }

  @Override
  public Set<String> getStorages() {
    return storageUpdatePeriods.keySet();
  }

  public List<String> getCubeNames() {
    return cubeNames;
  }

  /**
   * Return valid columns of the fact, which can be specified by the property
   * MetastoreUtil.getValidColumnsKey(getName())
   *
   * @return
   */
  public List<String> getValidColumns() {
    String validColsStr = getProperties().get(MetastoreUtil.getValidColumnsKey(
        getName()));
    return  validColsStr == null ? null : Arrays.asList(StringUtils.split(
        validColsStr.toLowerCase(), ','));
  }

  /**
   * Add update period to storage
   *
   * @param storage
   * @param period
   */
  public void addUpdatePeriod(String storage, UpdatePeriod period) {
    if (storageUpdatePeriods.containsKey(storage)) {
      storageUpdatePeriods.get(storage).add(period);
    } else {
      storageUpdatePeriods.put(storage, new HashSet<UpdatePeriod>(Arrays.asList(period)));
    }
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  /**
   * Remove update period from storage
   *
   * @param storage
   * @param period
   */
  public void removeUpdatePeriod(String storage, UpdatePeriod period) {
    if (storageUpdatePeriods.containsKey(storage)) {
      storageUpdatePeriods.get(storage).remove(period);
      addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
    }
  }

  /**
   * Alter a storage with specified update periods
   *
   * @param storage
   * @param updatePeriods
   * @throws HiveException
   */
  public void alterStorage(String storage, Set<UpdatePeriod> updatePeriods)
      throws HiveException {
    if (!storageUpdatePeriods.containsKey(storage)) {
      throw new HiveException("Invalid storage" + storage);
    }
    storageUpdatePeriods.put(storage, updatePeriods);
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  /**
   * Add a storage with specified update periods
   *
   * @param storage
   * @param updatePeriods
   * @throws HiveException
   */
  void addStorage(String storage, Set<UpdatePeriod> updatePeriods)
      throws HiveException {
    storageUpdatePeriods.put(storage, updatePeriods);
    addUpdatePeriodProperies(getName(), getProperties(), storageUpdatePeriods);
  }

  /**
   * Drop a storage from the fact
   *
   * @param storage
   */
  void dropStorage(String storage) {
    storageUpdatePeriods.remove(storage);
    getProperties().remove(MetastoreUtil.getFactUpdatePeriodKey(getName(), storage));
  }

  @Override
  public void alterColumn(FieldSchema column) throws HiveException {
    super.alterColumn(column);
  }

  @Override
  public void addColumns(Collection<FieldSchema> columns) throws HiveException {
    super.addColumns(columns);
  }

  /**
   * Add a cubeName to which this fact belongs
   *
   * @param cubeName
   */
  public void addCubeName(String cubeName) {
    cubeNames.add(cubeName.toLowerCase());
    addCubeNames(getName(), getProperties(), cubeNames);
  }

  /**
   * Remove a cubeName to which this fact no more belongs
   *
   * @param cubeName
   */
  public void removeCubeName(String cubeName) {
    cubeNames.remove(cubeName.toLowerCase());
    addCubeNames(getName(), getProperties(), cubeNames);
  }
}
