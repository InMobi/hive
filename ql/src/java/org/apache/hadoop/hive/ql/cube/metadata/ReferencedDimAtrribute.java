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
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class ReferencedDimAtrribute extends BaseDimAttribute {
  private final List<TableReference> references = new ArrayList<TableReference>();

  public ReferencedDimAtrribute(FieldSchema column, TableReference reference) {
    this(column, reference, null, null, null);
  }

  public ReferencedDimAtrribute(FieldSchema column, TableReference reference,
      Date startTime, Date endTime, Double cost) {
    super(column, startTime, endTime, cost);
    this.references.add(reference);
  }

  public ReferencedDimAtrribute(FieldSchema column,
      Collection<TableReference> references) {
    this(column, references, null, null, null);
  }

  public ReferencedDimAtrribute(FieldSchema column,
      Collection<TableReference> references, Date startTime, Date endTime,
      Double cost) {
    super(column, startTime, endTime, cost);
    this.references.addAll(references);
  }

  public void addReference(TableReference reference) {
    references.add(reference);
  }

  public List<TableReference> getReferences() {
    return references;
  }

  public boolean removeReference(TableReference ref) {
    return references.remove(ref);
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getDimensionSrcReferenceKey(getName()),
        MetastoreUtil.getDimensionDestReference(references));
  }

  /**
   * This is used only for serializing
   *
   * @param name
   * @param props
   */
  public ReferencedDimAtrribute(String name, Map<String, String> props) {
    super(name, props);
    String refListStr = props.get(MetastoreUtil.getDimensionSrcReferenceKey(
        getName()));
    String refListDims[] = StringUtils.split(refListStr, ",");
    for (String refDimRaw : refListDims) {
      references.add(new TableReference(refDimRaw));
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getReferences() == null) ? 0 :
        getReferences().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    ReferencedDimAtrribute other = (ReferencedDimAtrribute) obj;
    if (this.getReferences() == null) {
      if (other.getReferences() != null) {
        return false;
      }
    } else if (!this.getReferences().equals(other.getReferences())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "references:" + getReferences();
    return str;
  }
}
