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


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HierarchicalDimension extends CubeDimension {
  private final List<CubeDimension> hierarchy;

  public HierarchicalDimension(String name, List<CubeDimension> hierarchy) {
    super(name);
    this.hierarchy = hierarchy;
    assert (name != null);
    assert (hierarchy != null);
  }

  public List<CubeDimension> getHierarchy() {
    return hierarchy;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    for (int i = 0; i < hierarchy.size(); i++) {
      CubeDimension dim = hierarchy.get(i);
      props.put(MetastoreUtil.getHierachyElementKeyName(getName(), i),
          getHierarchyElement(dim));
      dim.addProperties(props);
    }
  }

  public static String getHierarchyElement(CubeDimension dim) {
    return dim.getName() + "," + dim.getClass().getCanonicalName();
  }

  public HierarchicalDimension(String name, Map<String, String> props) {
    super(name, props);
    this.hierarchy = getHiearachy(name, props);
  }

  public static List<CubeDimension> getHiearachy(String name,
      Map<String, String> props) {
    Map<Integer, String> hierarchyElements = new HashMap<Integer, String>();
    for (String param : props.keySet()) {
      if (param.startsWith(MetastoreUtil.getHierachyElementKeyPFX(name))) {
        hierarchyElements.put(MetastoreUtil.getHierachyElementIndex(name, param),
            props.get(param));
      }
    }
    List<CubeDimension> hierarchy = new ArrayList<CubeDimension>(
        hierarchyElements.size());
    for (int i = 0; i < hierarchyElements.size(); i++) {
      String hierarchyElement = hierarchyElements.get(i);
      String[] elements = hierarchyElement.split(",");
      String dimName = elements[0];
      String className = elements[1];
      CubeDimension dim;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        dim = (CubeDimension) constructor.newInstance(new Object[]
        {dimName, props});
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Invalid Dimension", e);
      }
      hierarchy.add(dim);
    }
    return hierarchy;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getHierarchy() == null) ? 0 :
        getHierarchy().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    HierarchicalDimension other = (HierarchicalDimension) obj;
    if (this.getHierarchy() == null) {
      if (other.getHierarchy() != null) {
        return false;
      }
    } else if (!this.getHierarchy().equals(other.getHierarchy())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += ", hierarchy:" + MetastoreUtil.getObjectStr(hierarchy);
    return str;
  }
}
