package org.apache.hadoop.hive.ql.cube.parse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class MultiFactHQLContext extends SimpleHQLContext {

  public static Log LOG = LogFactory.getLog(MultiFactHQLContext.class.getName());

  private Map<Dimension, CandidateDim> dimsToQuery;
  private Set<CandidateFact> facts;
  private CubeQueryContext query;
  private Map<CandidateFact, Set<Dimension>> factDimMap;

  MultiFactHQLContext(Set<CandidateFact> facts,
      Map<Dimension, CandidateDim> dimsToQuery,
      Map<CandidateFact, Set<Dimension>> factDimMap,
      CubeQueryContext query) throws SemanticException {
    super();
    this.query = query;
    this.facts = facts;
    this.dimsToQuery = dimsToQuery;
    this.factDimMap = factDimMap;
  }

  protected void setAll() throws SemanticException {
    setSelect(getSelectString());
    setFrom(getFromString());
    setWhere(getWhereString());
    setGroupby(getGroupbyString());
    setHaving(getHavingString());
    setOrderby(getOrderbyString());
  }

  private String getOrderbyString() {
    return query.getOrderByTree();
  }

  private String getHavingString() {
    return null;
  }

  private String getGroupbyString() {
    return null;
  }

  private String getWhereString() {
    return null;
  }

  public String toHQL() throws SemanticException {
    return query.getInsertClause() + super.toHQL();
  }

  private String getSelectString() throws SemanticException {
    Map<Integer, Integer> selectToFactIndex = 
        new HashMap<Integer, Integer>(query.getSelectAST().getChildCount());
    int fi = 1;
    for (CandidateFact fact : facts) {
      for(int ind : fact.getSelectIndices()) {
        if (!selectToFactIndex.containsKey(ind)) {
          selectToFactIndex.put(ind, fi);
        }
      }
      fi++;
    }
    StringBuilder select = new StringBuilder();
    for (int i = 0; i < query.getSelectAST().getChildCount(); i++) {
      if (selectToFactIndex.get(i) == null) {
        throw new SemanticException(ErrorMsg.EXPRESSION_NOT_IN_SINGLE_FACT,
            HQLParser.getString((ASTNode) query.getSelectAST().getChild(i)));
      }
      select.append("mq").append(selectToFactIndex.get(i)).append(".")
      .append(query.getSelectAlias(i)).append(" ").append(query.getSelectFinalAlias(i));
      if (i != query.getSelectAST().getChildCount() -1) {
        select.append(", ");
      }
    }
    return select.toString();
  }

  public Map<Dimension, CandidateDim> getDimsToQuery() {
    return dimsToQuery;
  }

  public Set<CandidateFact> getFactsToQuery() {
    return facts;
  }

  private String getFromString() throws SemanticException {
    StringBuilder fromBuilder = new StringBuilder();
    int aliasCount =1;
    Iterator<CandidateFact> iter = facts.iterator();
    while (iter.hasNext()) {
      CandidateFact fact = iter.next();
      FactHQLContext facthql = new FactHQLContext(fact, dimsToQuery,
          factDimMap.get(fact), query);
      fromBuilder.append("(");
      fromBuilder.append(facthql.toHQL());
      fromBuilder.append(")");
      fromBuilder.append(" mq" + aliasCount);
      aliasCount++;
      if (iter.hasNext()) {
        fromBuilder.append(" full outer join ");
      }
    }
    fromBuilder.append(" on ");
    CandidateFact firstFact = facts.iterator().next();
    for (int i = 2; i <= facts.size(); i++) {
      Iterator<Integer> dimIter = firstFact.getDimFieldIndices().iterator();
      while (dimIter.hasNext()) {
        String dim = query.getSelectAlias(dimIter.next());
        fromBuilder.append("mq1").append(".").append(dim).
        append(" = ").append("mq").append(i).append(".").append(dim);
        if (dimIter.hasNext()) {
          fromBuilder.append(" AND ");
        }
      }
      if (i != facts.size()) {
        fromBuilder.append(" AND ");        
      }
    }
    return fromBuilder.toString();
  }
}
