package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * HQL context class which passes all query strings from {@link CubeQueryContext}
 * and works with all dimensions to be queried.
 *
 * Updates from string with join clause expanded
 *
 */
class DimOnlyHQLContext extends DimHQLContext {

  public static Log LOG = LogFactory.getLog(DimOnlyHQLContext.class.getName());

  private final CubeQueryContext query;

  public CubeQueryContext getQuery() {
    return query;
  }

  DimOnlyHQLContext(Map<Dimension, CandidateDim> dimsToQuery,
      CubeQueryContext query) throws SemanticException {
    super(dimsToQuery, dimsToQuery.keySet(), query.getSelectTree(), query.getWhereTree(),
        query.getGroupByTree(), query.getOrderByTree(), query.getHavingTree(),
        query.getLimitValue());
    this.query = query;
  }

  protected void setMissingExpressions() throws SemanticException {
    setFrom(getFromString());
    super.setMissingExpressions();
  }

  public String toHQL() throws SemanticException {
    return query.getInsertClause() + super.toHQL();
  }

  protected String getFromTable() throws SemanticException {
    if (query.getAutoJoinCtx() != null &&
        query.getAutoJoinCtx().isJoinsResolved()) {
      return getDimsToQuery().get(query.getAutoJoinCtx().getAutoJoinTarget()).getStorageString(
          query.getAliasForTabName(query.getAutoJoinCtx().getAutoJoinTarget().getName()));
    } else {
      return query.getQBFromString(null, getDimsToQuery());
    }
  }

  private String getFromString() throws SemanticException {
    String fromString = null;
    String fromTable = getFromTable();
    if (query.getAutoJoinCtx() != null &&
        query.getAutoJoinCtx().isJoinsResolved()) {
      fromString = query.getAutoJoinCtx().getFromString(fromTable, null,
          getDimsToQuery().keySet(), getDimsToQuery(), query);
    } else {
      fromString = fromTable;
    }
    return fromString;
  }
}
