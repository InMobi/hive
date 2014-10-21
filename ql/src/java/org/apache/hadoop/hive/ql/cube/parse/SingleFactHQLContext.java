package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class SingleFactHQLContext extends DimOnlyHQLContext {

  public static Log LOG = LogFactory.getLog(SingleFactHQLContext.class.getName());

  private CandidateFact fact;

  SingleFactHQLContext(CandidateFact fact,
      Map<Dimension, CandidateDim> dimsToQuery, CubeQueryContext query) throws SemanticException {
    super(dimsToQuery, query);
    this.fact = fact;
  }

  public CandidateFact getFactToQuery() {
    return fact;
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

  private final String unionQueryFormat = "SELECT * FROM %s";
  String getUnionQueryFormat() {
    StringBuilder queryFormat = new StringBuilder();
    queryFormat.append(unionQueryFormat);
    if (getQuery().getGroupByTree() != null) {
      queryFormat.append(" GROUP BY %s");
    }
    if (getQuery().getHavingTree() != null) {
      queryFormat.append(" HAVING %s");
    }
    if (getQuery().getOrderByTree() != null) {
      queryFormat.append(" ORDER BY %s");
    }
    if (getQuery().getLimitValue() != null) {
      queryFormat.append(" LIMIT %s");
    }
    return queryFormat.toString();
  }

  protected String getFromTable() throws SemanticException {
    if (getQuery().getAutoJoinCtx() != null &&
        getQuery().getAutoJoinCtx().isJoinsResolved()) {
      return fact.getStorageString(
          getQuery().getAliasForTabName(getQuery().getCube().getName()));
    } else {
      return getQuery().getQBFromString(fact, getDimsToQuery());
    }
  }
}
