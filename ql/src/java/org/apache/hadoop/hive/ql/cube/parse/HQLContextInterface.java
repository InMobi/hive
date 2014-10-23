package org.apache.hadoop.hive.ql.cube.parse;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * HQL context holding the ql expressions 
 */
public interface HQLContextInterface {

  /**
   * Get the HQL query.
   * 
   * @return query string
   * @throws SemanticException
   */
  public String toHQL() throws SemanticException;

  /**
   * Get select expression.
   *
   * @return select
   */
  public String getSelect();

  /**
   * Get from string
   *
   * @return from
   */
  public String getFrom();

  /**
   * Get where string
   *
   * @return where
   */
  public String getWhere();

  /**
   * Get groupby string
   *
   * @return groupby
   */
  public String getGroupby();

  /**
   * Get having string
   *
   * @return having
   */
  public String getHaving();

  /**
   * Get orderby string
   *
   * @return orderby
   */
  public String getOrderby();

  /**
   * Get limit
   *
   * @return limit
   */
  public Integer getLimit();
}
