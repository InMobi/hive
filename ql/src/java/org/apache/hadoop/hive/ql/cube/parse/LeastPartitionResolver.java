package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
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
      Map<CubeFactTable, Integer> numPartitionsMap =
          cubeql.getFactPartitionMap();

      int minPartitions = Collections.min(numPartitionsMap.values());

      for (Iterator<CubeFactTable> i =
          cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CubeFactTable fact = i.next();
        if (numPartitionsMap.get(fact) > minPartitions) {
          LOG.info("Not considering fact:" + fact +
              " from candidate fact tables as it requires more partitions to" +
              " be queried:" + numPartitionsMap.get(fact) + " minimum:"
              + minPartitions);
          i.remove();
        }
      }
    }
  }
}
