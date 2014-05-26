package org.apache.hadoop.hive.ql.cube.metadata;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public final class CubeDimensionTable extends AbstractCubeTable {
  private String uberDim;
  private final Map<String, UpdatePeriod> snapshotDumpPeriods = new HashMap<String, UpdatePeriod>();

  public CubeDimensionTable(String uberDim, String dimName, List<FieldSchema> columns,
      double weight, Map<String, UpdatePeriod> snapshotDumpPeriods) {
    this(uberDim, dimName, columns, weight, snapshotDumpPeriods,
        new HashMap<String, String>());
  }

  public CubeDimensionTable(String uberDim, String dimName, List<FieldSchema> columns,
      double weight, Set<String> storages) {
    this(uberDim, dimName, columns, weight, getSnapshotDumpPeriods(storages),
         new HashMap<String, String>());
  }

  public CubeDimensionTable(String uberDim, String dimName, List<FieldSchema> columns,
      double weight, Set<String> storages,
      Map<String, String> properties) {
    this(uberDim, dimName, columns, weight, getSnapshotDumpPeriods(storages),
         properties);
  }

  public CubeDimensionTable(String uberDim, String dimName, List<FieldSchema> columns,
      double weight,
      Map<String, UpdatePeriod> snapshotDumpPeriods,
      Map<String, String> properties) {
    super(dimName, columns, properties, weight);
    this.uberDim = uberDim;
    if (snapshotDumpPeriods != null) {
      this.snapshotDumpPeriods.putAll(snapshotDumpPeriods);
    }
    addProperties();
  }

  private static Map<String, UpdatePeriod> getSnapshotDumpPeriods(
      Set<String> storages) {
    Map<String, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<String, UpdatePeriod>();
    for (String storage : storages) {
      snapshotDumpPeriods.put(storage, null);
    }
    return snapshotDumpPeriods;
  }

  public CubeDimensionTable(Table tbl) {
    super(tbl);
    this.uberDim = getUberDimName(getName(), getProperties());
    Map<String, UpdatePeriod> dumpPeriods = getDumpPeriods(getName(), getProperties());
    if (dumpPeriods != null) {
      this.snapshotDumpPeriods.putAll(dumpPeriods);
    }
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.DIMENSION;
  }

  @Override
  protected void addProperties() {
    super.addProperties();
    setUberDimName(getProperties(), getName(), uberDim);
    setSnapshotPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }

  public Map<String, UpdatePeriod> getSnapshotDumpPeriods() {
    return snapshotDumpPeriods;
  }

  public String getUberDim() {
    return uberDim;
  }

  private static void setSnapshotPeriods(String name, Map<String, String> props,
                                        Map<String, UpdatePeriod> snapshotDumpPeriods) {
    if (snapshotDumpPeriods != null) {
      props.put(MetastoreUtil.getDimensionStorageListKey(name),
          MetastoreUtil.getStr(snapshotDumpPeriods.keySet()));
      for (Map.Entry<String, UpdatePeriod> entry : snapshotDumpPeriods.entrySet())
      {
        if (entry.getValue() != null) {
          props.put(MetastoreUtil.getDimensionDumpPeriodKey(name, entry.getKey()),
              entry.getValue().name());
        }
      }
    }
  }

  private static void setUberDimName(Map<String, String> props, String dimName, String uberDimName) {
    props.put(MetastoreUtil.getUberDimNameKey(dimName), uberDimName);
  }

  static String getUberDimName(String dimName, Map<String, String> props) {
    return props.get(MetastoreUtil.getUberDimNameKey(dimName));
  }

  private static Map<String, UpdatePeriod> getDumpPeriods(String name,
      Map<String, String> params) {
    String storagesStr = params.get(MetastoreUtil.getDimensionStorageListKey(
        name));
    if (storagesStr != null) {
      Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
      String[] storages = storagesStr.split(",");
      for (String storage : storages) {
        String dumpPeriod = params.get(MetastoreUtil.getDimensionDumpPeriodKey(
            name, storage));
        if (dumpPeriod != null) {
          dumpPeriods.put(storage, UpdatePeriod.valueOf(dumpPeriod));
        } else {
          dumpPeriods.put(storage, null);
        }
      }
      return dumpPeriods;
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CubeDimensionTable other = (CubeDimensionTable) obj;

    if (this.getUberDim() == null) {
      if (other.getUberDim() != null) {
        return false;
      }
    } else {
      if (!this.getUberDim().equals(
          other.getUberDim())) {
        return false;
      }
    }
    if (this.getSnapshotDumpPeriods() == null) {
      if (other.getSnapshotDumpPeriods() != null) {
        return false;
      }
    } else {
      if (!this.getSnapshotDumpPeriods().equals(
          other.getSnapshotDumpPeriods())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Set<String> getStorages() {
    return snapshotDumpPeriods.keySet();
  }

  public boolean hasStorageSnapshots(String storage) {
    return (snapshotDumpPeriods.get(storage) != null);
  }

  /**
   * Alter the uber dim name
   * 
   * @param newUberDimName
   */
  public void alterUberDim(String newUberDimName) {
    this.uberDim = newUberDimName;
    setUberDimName(getProperties(), getName(), this.uberDim);
  }

  /**
   * Alter snapshot dump period of a storage
   *
   * @param storage Storage name
   * @param period The new value
   * @throws HiveException
   */
  public void alterSnapshotDumpPeriod(String storage,
      UpdatePeriod period) throws HiveException {
    if (storage == null) {
      throw new HiveException("Cannot add null storage for " + getName());
    }

    if (snapshotDumpPeriods.containsKey(storage)) {
      LOG.info("Updating dump period for " + storage + " from "
        + snapshotDumpPeriods.get(storage) + " to " + period);
    }

    snapshotDumpPeriods.put(storage, period);
    setSnapshotPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }

  @Override
  public void alterColumn(FieldSchema column) throws HiveException {
    super.alterColumn(column);
  }

  @Override
  public void addColumns(Collection<FieldSchema> columns) throws HiveException {
    super.addColumns(columns);
  }

  void dropStorage(String storage) {
    snapshotDumpPeriods.remove(storage);
    setSnapshotPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }

  /**
   * @return the timedDimension
   */
  public String getTimedDimension() {
    return getProperties().get(MetastoreConstants.TIMED_DIMENSION);
  }

}
