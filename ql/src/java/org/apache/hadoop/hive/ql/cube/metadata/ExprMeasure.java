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


import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public final class ExprMeasure extends CubeMeasure {
  private final String expr;

  public ExprMeasure(FieldSchema column, String expr, String formatString,
      String aggregate, String unit) {
    this(column, expr, formatString, aggregate, unit, null, null, null);
  }

  public ExprMeasure(FieldSchema column, String expr, String formatString,
      String aggregate, String unit, Date startTime, Date endTime, Double cost) {
    super(column, formatString, aggregate, unit, startTime, endTime, cost);
    this.expr = expr;
    assert (expr != null);
  }

  public ExprMeasure(FieldSchema column, String expr) {
    this(column, expr, null, null, null);
  }

  public ExprMeasure(String name, Map<String, String> props) {
    super(name, props);
    this.expr = props.get(MetastoreUtil.getMeasureExprPropertyKey(getName()));
  }

  public String getExpr() {
    return expr;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getMeasureExprPropertyKey(getName()), expr);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getExpr() == null) ? 0 :
        getExpr().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    ExprMeasure other = (ExprMeasure) obj;
    if (this.getExpr() == null) {
      if (other.getExpr() != null) {
        return false;
      }
    } else if (!this.getExpr().equalsIgnoreCase(other.getExpr())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "expr:" + expr;
    return str;
  }
}
