/**
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
package org.apache.hadoop.hive.ql.cube.metadata;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class UberDimension extends AbstractCubeTable {

  private final Set<CubeDimension> attributes;
  private static final List<FieldSchema> columns = new ArrayList<FieldSchema>();
  private final Map<String, CubeDimension> attributeMap;

  static {
    columns.add(new FieldSchema("dummy", "string", "dummy column"));
  }

  public UberDimension(String name, 
      Set<CubeDimension> attributes) {
    this(name, attributes, new HashMap<String, String>(), 0L);
  }

  public UberDimension(String name, 
      Set<CubeDimension> attributes, Map<String, String> properties,
      double weight) {
    super(name, columns, properties, weight);
    this.attributes = attributes;

    attributeMap = new HashMap<String, CubeDimension>();
    for (CubeDimension dim : attributes) {
      attributeMap.put(dim.getName().toLowerCase(), dim);
    }

    addProperties();
  }

  public UberDimension(Table tbl) {
    super(tbl);
    this.attributes = getAttributes(getName(), getProperties());

    attributeMap = new HashMap<String, CubeDimension>();
    for (CubeDimension attr : attributes) {
      addAllAttributesToMap(attr);
    }
  }

  private void addAllAttributesToMap(CubeDimension attr) {
    attributeMap.put(attr.getName().toLowerCase(), attr);
    if (attr instanceof HierarchicalDimension) {
      for (CubeDimension d : ((HierarchicalDimension)attr).getHierarchy()) {
        addAllAttributesToMap(d);
      }
    }
  }

  public Set<CubeDimension> getAttributes() {
    return attributes;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.UBER_DIMENSION;
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }

  @Override
  public void addProperties() {
    super.addProperties();
    getProperties().put(MetastoreUtil.getUberDimAttributeListKey(getName()),
        MetastoreUtil.getNamedStr(attributes));
    setAttributedProperties(getProperties(), attributes);
  }

  private static void setAttributedProperties(Map<String, String> props,
                                             Set<CubeDimension> attributes) {
    for (CubeDimension attr : attributes) {
      attr.addProperties(props);
    }
  }

  public static Set<CubeDimension> getAttributes(String name,
      Map<String, String> props) {
    Set<CubeDimension> attributes = new HashSet<CubeDimension>();
    String attrStr = props.get(MetastoreUtil.getUberDimAttributeListKey(name));
    String[] names = attrStr.split(",");
    for (String attrName : names) {
      String className = props.get(MetastoreUtil.getDimensionClassPropertyKey(
          attrName));
      CubeDimension attr;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        attr = (CubeDimension) constructor.newInstance(new Object[]
            {attrName, props});
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      }
      attributes.add(attr);
    }
    return attributes;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    UberDimension other = (UberDimension) obj;
    if (this.getAttributes() == null) {
      if (other.getAttributes() != null) {
        return false;
      }
    } else if (!this.getAttributes().equals(other.getAttributes())) {
      return false;
    }
    return true;
  }

  public CubeDimension getAttributeByName(String attr) {
    return attributeMap.get(attr == null ? attr : attr.toLowerCase());
  }

  public CubeColumn getColumnByName(String column) {
    return getAttributeByName(column);
  }

  /**
   * Alters the attribute if already existing or just adds if it is new attribute
   *
   * @param dimension
   * @throws HiveException
   */
  public void alterAttribute(CubeDimension attribute) throws HiveException {
    if (attribute == null) {
      throw new NullPointerException("Cannot add null attribute");
    }

    // Replace dimension if already existing
    if (attributeMap.containsKey(attribute.getName().toLowerCase())) {
      attributes.remove(getAttributeByName(attribute.getName()));
      LOG.info("Replacing attribute " + getAttributeByName(attribute.getName())
        + " with " + attribute);
    }

    attributes.add(attribute);
    attributeMap.put(attribute.getName().toLowerCase(), attribute);
    getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
        MetastoreUtil.getNamedStr(attributes));
    attribute.addProperties(getProperties());
  }

  /**
   * Remove the dimension with name specified
   *
   * @param attrName
   */
  public void removeAttribute(String attrName) {
    if (attributeMap.containsKey(attrName.toLowerCase())) {
      LOG.info("Removing dimension " + getAttributeByName(attrName));
      attributes.remove(getAttributeByName(attrName));
      attributeMap.remove(attrName.toLowerCase());
      getProperties().put(MetastoreUtil.getCubeDimensionListKey(getName()),
          MetastoreUtil.getNamedStr(attributes));
    }
  }
}
