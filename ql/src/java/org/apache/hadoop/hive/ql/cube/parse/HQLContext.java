package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateDim;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateFact;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HQLContext {

  public static Log LOG = LogFactory.getLog(HQLContext.class.getName());

  private Map<Dimension, CandidateDim> dimsToQuery;
  private Set<CandidateFact> facts;
  private CubeQueryContext query;

  HQLContext(Set<CandidateFact> facts, Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query) {
    this.query = query;
    this.facts = facts;
    this.dimsToQuery = dimsToQuery;
  }

  public String toHQL() throws SemanticException {
    String fromString = getFromString();
    String whereString = genWhereClauseWithDimPartitions();
    String qfmt = getQueryFormat(whereString);
    Object[] queryTreeStrings = getQueryTreeStrings(fromString, whereString);
    if (LOG.isDebugEnabled()) {
      LOG.debug("qfmt:" + qfmt + " Query strings: " + Arrays.toString(queryTreeStrings));
    }
    String baseQuery = String.format(qfmt, queryTreeStrings);
    return query.getInsertClause() + baseQuery;
  }

  public Map<Dimension, CandidateDim> getDimsToQuery() {
    return dimsToQuery;
  }

  public Set<CandidateFact> getFactsToQuery() {
    return facts;
  }

  private Object[] getQueryTreeStrings(String fromString, String whereString)
      throws SemanticException {
    List<String> qstrs = new ArrayList<String>();
    qstrs.add(query.getSelectTree());
    qstrs.add(fromString);
    if (!StringUtils.isBlank(whereString)) {
      qstrs.add(whereString);
    }
    if (query.getGroupByTree() != null) {
      qstrs.add(query.getGroupByTree());
    }
    if (query.getHavingTree() != null) {
      qstrs.add(query.getHavingTree());
    }
    if (query.getOrderByTree() != null) {
      qstrs.add(query.getOrderByTree());
    }
    if (query.getLimitValue() != null) {
      qstrs.add(String.valueOf(query.getLimitValue()));
    }
    return qstrs.toArray(new String[0]);
  }

  static void addRangeClauses(CubeQueryContext query, CandidateFact fact) throws SemanticException {
    if (fact != null) {
      // resolve timerange positions and replace it by corresponding where clause
      for (TimeRange range : query.getTimeRanges()) {
        String rangeWhere = fact.rangeToWhereClause.get(range);
        if (!StringUtils.isBlank(rangeWhere)) {
          ASTNode rangeAST;
          try {
            rangeAST = HQLParser.parseExpr(rangeWhere);
          } catch (ParseException e) {
            throw new SemanticException(e);
          }
          rangeAST.setParent(range.getParent());
          range.getParent().setChild(range.getChildIndex(), rangeAST);
        }
      }
    }
  }

  private String genWhereClauseWithDimPartitions() {
    String originalWhere = query.getWhereTree();
    StringBuilder whereBuf;
    if (originalWhere != null) {
      whereBuf = new StringBuilder(originalWhere);
    } else {
      whereBuf = new StringBuilder();
    }

    // add where clause for all dimensions
    if (dimsToQuery != null) {
      boolean added = (originalWhere != null || fact != null);
      for (Map.Entry<Dimension, CandidateDim> entry : dimsToQuery.entrySet()) {
        CandidateDim cdim = entry.getValue();
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

  private final String baseQueryFormat = "SELECT %s FROM %s";

  String getQueryFormat(String whereString) {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(baseQueryFormat);
    if (!StringUtils.isBlank(whereString)) {
      queryFormat.append(" WHERE %s");
    }
    if (query.getGroupByTree() != null) {
      queryFormat.append(" GROUP BY %s");
    }
    if (query.getHavingTree() != null) {
      queryFormat.append(" HAVING %s");
    }
    if (query.getOrderByTree() != null) {
      queryFormat.append(" ORDER BY %s");
    }
    if (query.getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  private final String unionQueryFormat = "SELECT * FROM %s";
  String getUnionQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(unionQueryFormat);
    if (query.getGroupByTree() != null) {
      queryFormat.append(" GROUP BY %s");
    }
    if (query.getHavingTree() != null) {
      queryFormat.append(" HAVING %s");
    }
    if (query.getOrderByTree() != null) {
      queryFormat.append(" ORDER BY %s");
    }
    if (query.getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  private String getFromString() throws SemanticException {
    String fromString = null;
    if (query.getAutoJoinCtx() != null &&
        query.getAutoJoinCtx().isJoinsResolved()) {
      fromString = query.getAutoJoinCtx().getFromString(this, query);
    } else {
      fromString = query.getQBFromString(fact, dimsToQuery);
    }
    return fromString;
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
