package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateFact;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LeastDimensionResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(
      LeastDimensionResolver.class.getName());

  public LeastDimensionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    if (cubeql.getCube() != null && !cubeql.getCandidateFactTables()
        .isEmpty()) {
      Map<CandidateFact, Integer> dimWeightMap =
          new HashMap<CandidateFact, Integer>();

      for (CandidateFact fact : cubeql.getCandidateFactTables()) {
        dimWeightMap.put(fact, getDimensionWeight(cubeql, fact));
      }

      int minWeight = Collections.min(dimWeightMap.values());

      for (Iterator<CandidateFact> i =
          cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CandidateFact fact = i.next();
        if (dimWeightMap.get(fact) > minWeight) {
          LOG.info("Removing fact:" + fact +
              " from candidate fact tables as it has more dimension weight:"
              + dimWeightMap.get(fact) + " minimum:"
              + minWeight);
          i.remove();
        }
      }
    }
  }

  private Integer getDimensionWeight(CubeQueryContext cubeql,
      CandidateFact fact) {
    // TODO get the dimension weight associated with the fact wrt query
    return 0;
  }

}
