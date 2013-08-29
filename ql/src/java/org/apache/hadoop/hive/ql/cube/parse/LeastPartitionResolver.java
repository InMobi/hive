package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateFact;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LeastPartitionResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(
      LeastPartitionResolver.class.getName());

  public LeastPartitionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    if (cubeql.getCube() != null && !cubeql.getCandidateFactTables().isEmpty())
    {
      int minPartitions = getMinPartitions(cubeql.getCandidateFactTables());

      for (Iterator<CandidateFact> i =
          cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CandidateFact fact = i.next();
        if (fact.numQueriedParts > minPartitions) {
          LOG.info("Not considering fact:" + fact +
              " from candidate fact tables as it requires more partitions to" +
              " be queried:" + fact.numQueriedParts + " minimum:"
              + minPartitions);
          i.remove();
        }
      }
    }
  }

  int getMinPartitions(Set<CandidateFact> candidateFacts) {
    Iterator<CandidateFact> it = candidateFacts.iterator();
    int min = it.next().numQueriedParts;
    while (it.hasNext()) {
      CandidateFact fact = it.next();
      if (fact.numQueriedParts < min) {
        min = fact.numQueriedParts;
      }
    }
    return min;
  }
}
