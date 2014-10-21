package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.cube.metadata.Dimension;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class HQLContext {

  public static Log LOG = LogFactory.getLog(HQLContext.class.getName());

  private HQLContextInterface hql;

  HQLContext(Set<CandidateFact> facts, Map<Dimension, CandidateDim> dimsToQuery,
      Map<CandidateFact, Set<Dimension>> factDimMap, CubeQueryContext query)
          throws SemanticException {
    if (facts == null || facts.size() == 0) {
      hql = new DimOnlyHQLContext(dimsToQuery, query);
    } else if (facts.size() == 1) {
      // create singlefact context
      hql = new SingleFactHQLContext(facts.iterator().next(), dimsToQuery, query);
    } else {
      hql = new MultiFactHQLContext(facts,
          dimsToQuery, factDimMap, query);
    }
  }

  public String toHQL() throws SemanticException {
    return hql.toHQL();
  }

  public HQLContextInterface getHQL() {
    return hql;
  }
}
