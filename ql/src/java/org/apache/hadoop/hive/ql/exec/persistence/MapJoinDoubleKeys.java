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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator.HashTableSinkObjectCtx;
import org.apache.hadoop.hive.ql.exec.MapJoinMetaData;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

public class MapJoinDoubleKeys extends AbstractMapJoinKey {

  protected transient Object obj1;
  protected transient Object obj2;


  public MapJoinDoubleKeys() {
  }

  /**
   * @param obj1
   * @param obj2
   */
  public MapJoinDoubleKeys(Object obj1, Object obj2) {
    this.obj1 = obj1;
    this.obj2 = obj2;
  }



  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((obj1 == null) ? 0 : obj1.hashCode());
    result = prime * result + ((obj2 == null) ? 0 : obj2.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MapJoinDoubleKeys other = (MapJoinDoubleKeys) obj;
    if (obj1 == null) {
      if (other.obj1 != null) {
        return false;
      }
    } else if (!obj1.equals(other.obj1)) {
      return false;
    }
    if (obj2 == null) {
      if (other.obj2 != null) {
        return false;
      }
    } else if (!obj2.equals(other.obj2)) {
      return false;
    }
    return true;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    try {
      // get the tableDesc from the map stored in the mapjoin operator
      HashTableSinkObjectCtx ctx = MapJoinOperator.getMetadata().get(Integer.valueOf(metadataTag));

      Writable val = ctx.getSerDe().getSerializedClass().newInstance();
      val.readFields(in);



      ArrayList<Object> list = (ArrayList<Object>) ObjectInspectorUtils.copyToStandardObject(ctx
          .getSerDe().deserialize(val), ctx.getSerDe().getObjectInspector(),
          ObjectInspectorCopyOption.WRITABLE);

      if (list == null) {
        obj1 = null;
        obj2 = null;

      } else {
        obj1 = list.get(0);
        obj2 = list.get(1);
      }

    } catch (Exception e) {
      throw new IOException(e);
    }

  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    try {
      // out.writeInt(metadataTag);
      // get the tableDesc from the map stored in the mapjoin operator
      HashTableSinkObjectCtx ctx = HashTableSinkOperator.getMetadata().get(
          Integer.valueOf(metadataTag));

      ArrayList<Object> list = MapJoinMetaData.getList();
      list.add(obj1);
      list.add(obj2);
      // Different processing for key and value
      Writable outVal = ctx.getSerDe().serialize(list, ctx.getStandardOI());
      outVal.write(out);

    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }



  /**
   * @return the obj
   */
  public Object getObj1() {
    return obj1;
  }

  /**
   * @param obj1
   *          the obj to set
   */
  public void setObj1(Object obj1) {
    this.obj1 = obj1;
  }

  /**
   * @return the obj
   */
  public Object getObj2() {
    return obj2;
  }

  /**
   * @param obj2
   *          the obj to set
   */
  public void setObj2(Object obj2) {
    this.obj2 = obj2;
  }


  @Override
  public boolean hasAnyNulls(boolean[] nullsafes) {
    if (obj1 == null && (nullsafes == null || !nullsafes[0])) {
      return true;
    }
    if (obj2 == null && (nullsafes == null || !nullsafes[1])) {
      return true;
    }
    return false;
  }
}
