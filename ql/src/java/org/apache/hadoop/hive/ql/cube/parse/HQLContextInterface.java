package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.hive.ql.parse.SemanticException;

public interface HQLContextInterface {

  public String toHQL() throws SemanticException;

  public String getSelect();

  public String getFrom();

  public String getWhere();

  public String getGroupby();

  public String getHaving();

  public String getOrderby();

  public Integer getLimit();
}
