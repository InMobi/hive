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
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator.HashTableSinkObjectCtx;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

/**
 * Map Join Object used for both key.
 */
public class MapJoinObjectKey extends AbstractMapJoinKey {


  protected transient Object[] obj;

  public MapJoinObjectKey() {
  }

  /**
   * @param obj
   */
  public MapJoinObjectKey(Object[] obj) {
    this.obj = obj;
  }



  @Override
  public int hashCode() {
    return Arrays.hashCode(obj);
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
    MapJoinObjectKey other = (MapJoinObjectKey) obj;
    if (!Arrays.equals(this.obj, other.obj)) {
      return false;
    }
    return true;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    try {
      // get the tableDesc from the map stored in the mapjoin operator
      HashTableSinkObjectCtx ctx = MapJoinOperator.getMetadata().get(
          Integer.valueOf(metadataTag));

      Writable val = ctx.getSerDe().getSerializedClass().newInstance();
      val.readFields(in);
      ArrayList<Object> list = (ArrayList<Object>) ObjectInspectorUtils.copyToStandardObject(ctx
          .getSerDe().deserialize(val), ctx.getSerDe().getObjectInspector(),
          ObjectInspectorCopyOption.WRITABLE);
      if (list == null) {
        obj = new ArrayList(0).toArray();
      } else {
        obj = list.toArray();
      }

    } catch (Exception e) {
      throw new IOException(e);
    }

  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    try {
      // get the tableDesc from the map stored in the mapjoin operator
      HashTableSinkObjectCtx ctx = HashTableSinkOperator.getMetadata().get(
          Integer.valueOf(metadataTag));

      // Different processing for key and value
      Writable outVal = ctx.getSerDe().serialize(obj, ctx.getStandardOI());
      outVal.write(out);
    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }


  /**
   * @return the obj
   */
  public Object[] getObj() {
    return obj;
  }

  /**
   * @param obj
   *          the obj to set
   */
  public void setObj(Object[] obj) {
    this.obj = obj;
  }

  @Override
  public boolean hasAnyNulls(boolean[] nullsafes) {
    if (obj != null && obj.length > 0) {
      for (int i = 0; i < obj.length; i++) {
        if (obj[i] == null && (nullsafes == null || !nullsafes[i])) {
          return true;
        }
      }
    }
    return false;

  }

}
