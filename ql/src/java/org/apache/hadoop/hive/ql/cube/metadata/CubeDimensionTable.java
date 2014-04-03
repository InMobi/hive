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
  private final Map<String, List<TableReference>> dimensionReferences = new HashMap<String, List<TableReference>>();
  private final Map<String, UpdatePeriod> snapshotDumpPeriods = new HashMap<String, UpdatePeriod>();

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      double weight, Map<String, UpdatePeriod> snapshotDumpPeriods) {
    this(dimName, columns, weight, snapshotDumpPeriods,
        new HashMap<String, List<TableReference>>(), new HashMap<String, String>());
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      double weight, Set<String> storages) {
    this(dimName, columns, weight, getSnapshotDumpPeriods(storages),
        new HashMap<String, List<TableReference>>(), new HashMap<String, String>());
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      double weight, Map<String, UpdatePeriod> snapshotDumpPeriods,
      Map<String, List<TableReference>> dimensionReferences) {
    this(dimName, columns, weight, snapshotDumpPeriods, dimensionReferences,
        new HashMap<String, String>());
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      double weight, Set<String> storages,
      Map<String, List<TableReference>> dimensionReferences) {
    this(dimName, columns, weight, getSnapshotDumpPeriods(storages),
        dimensionReferences, new HashMap<String, String>());
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      double weight, Set<String> storages,
      Map<String, List<TableReference>> dimensionReferences,
      Map<String, String> properties) {
    this(dimName, columns, weight, getSnapshotDumpPeriods(storages),
        dimensionReferences, properties);
  }

  public CubeDimensionTable(String dimName, List<FieldSchema> columns,
      double weight,
      Map<String, UpdatePeriod> snapshotDumpPeriods,
      Map<String, List<TableReference>> dimensionReferences,
      Map<String, String> properties) {
    super(dimName, columns, properties, weight);
    if (dimensionReferences != null) {
      this.dimensionReferences.putAll(dimensionReferences);
    }
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
    Map<String, List<TableReference>> dimRefs = getDimensionReferences(getProperties());
    if (dimRefs != null) {
      this.dimensionReferences.putAll(dimRefs);
    }
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
    setDimensionReferenceProperties(getProperties(), dimensionReferences);
    setSnapshotPeriods(getName(), getProperties(), snapshotDumpPeriods);
  }

  public Map<String, List<TableReference>> getDimensionReferences() {
    return dimensionReferences;
  }

  public Map<String, UpdatePeriod> getSnapshotDumpPeriods() {
    return snapshotDumpPeriods;
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

  private static void setDimensionReferenceProperties(Map<String, String> props,
                                                     Map<String, List<TableReference>> dimensionReferences) {
    if (dimensionReferences != null) {
      for (Map.Entry<String, List<TableReference>> entry : dimensionReferences.entrySet()) {
        props.put(MetastoreUtil.getDimensionSrcReferenceKey(entry.getKey()),
            MetastoreUtil.getDimensionDestReference(entry.getValue()));
      }
    }
  }

  private static Map<String, List<TableReference>> getDimensionReferences(
      Map<String, String> params) {
    Map<String, List<TableReference>> dimensionReferences =
        new HashMap<String, List<TableReference>>();
    for (String param : params.keySet()) {
      if (param.startsWith(MetastoreConstants.DIM_KEY_PFX)) {
        String key = param.replace(MetastoreConstants.DIM_KEY_PFX, "");
        String toks[] = key.split("\\.+");
        String dimName = toks[0];
        String value = params.get(MetastoreUtil.getDimensionSrcReferenceKey(dimName));

        if (value != null) {
          String refDims[] = StringUtils.split(value, ",");
          List<TableReference> references = new ArrayList<TableReference>(refDims.length);
          for (String refDim : refDims) {
            references.add(new TableReference(refDim));
          }
          dimensionReferences.put(dimName, references);
        }
      }
    }
    return dimensionReferences;
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

    if (this.getDimensionReferences() == null) {
      if (other.getDimensionReferences() != null) {
        return false;
      }
    } else {
      if (!this.getDimensionReferences().equals(
          other.getDimensionReferences())) {
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
   * Add a table reference to dimension column
   *
   * @param referenceName The column name
   * @param reference The new reference
   *
   * @throws HiveException
   */
  public void addDimensionReference(String referenceName,
      TableReference reference) throws HiveException {
    List<TableReference> refs = dimensionReferences.get(referenceName);
    if (refs != null) {
      Iterator<TableReference> itr = refs.iterator();
      while (itr.hasNext()) {
        TableReference existing = itr.next();
        if (existing.equals(reference)) {
          itr.remove();
        }
      }
    } else {
      refs = new ArrayList<TableReference>(1);
      dimensionReferences.put(referenceName, refs);
    }
    refs.add(reference);
    setDimensionReferenceProperties(getProperties(), dimensionReferences);
  }

  /**
   * Add or Alter the references of a dimension column
   *
   * @param referenceName The column name
   * @param reference The new reference
   *
   * @throws HiveException
   */
  public void alterDimensionReferences(String referenceName,
      List<TableReference> references) throws HiveException {
    dimensionReferences.put(referenceName, references);
    setDimensionReferenceProperties(getProperties(), dimensionReferences);
  }

  /**
   * Remove all the dimension references of specified dimension column
   *
   * @param referenceName
   */
  public void removeDimensionRefernces(String referenceName) {
    dimensionReferences.remove(referenceName);
    setDimensionReferenceProperties(getProperties(), dimensionReferences);
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
