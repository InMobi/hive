package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public abstract class DimHQLContext extends SimpleHQLContext {

  public static Log LOG = LogFactory.getLog(DimHQLContext.class.getName());

  private final Map<Dimension, CandidateDim> dimsToQuery;
  private final Set<Dimension> queriedDims;
  private String where;

  DimHQLContext(Map<Dimension, CandidateDim> dimsToQuery, Set<Dimension> queriedDims,
      String select, String where, String groupby,
      String orderby, String having, Integer limit) throws SemanticException {
    super(select, groupby, orderby, having, limit);
    this.dimsToQuery = dimsToQuery;
    this.where = where;
    this.queriedDims = queriedDims;
  }

  protected void setAll() throws SemanticException {
    setWhere(genWhereClauseWithDimPartitions(where));    
  }

  public Map<Dimension, CandidateDim> getDimsToQuery() {
    return dimsToQuery;
  }

  private String genWhereClauseWithDimPartitions(String originalWhere) {
    StringBuilder whereBuf;
    if (originalWhere != null) {
      whereBuf = new StringBuilder(originalWhere);
    } else {
      whereBuf = new StringBuilder();
    }

    // add where clause for all dimensions
    if (queriedDims != null) {
      boolean added = (originalWhere != null);
      for (Dimension dim : queriedDims) {
        CandidateDim cdim = dimsToQuery.get(dim);
        if (!cdim.isWhereClauseAdded()) {
          appendWhereClause(whereBuf, cdim.whereClause, added);
          added = true;
        }
      }
    }
    if (whereBuf.length() == 0) {
      return null;
    }
    return whereBuf.toString();
  }

  static void appendWhereClause(StringBuilder filterCondition,
      String whereClause, boolean hasMore) {
    // Make sure we add AND only when there are already some conditions in where clause
    if (hasMore && !filterCondition.toString().isEmpty()
        && !StringUtils.isBlank(whereClause)) {
      filterCondition.append(" AND ");
    }

    if (!StringUtils.isBlank(whereClause)) {
      filterCondition.append("(");
      filterCondition.append(whereClause);
      filterCondition.append(")");
    }
  }
}
