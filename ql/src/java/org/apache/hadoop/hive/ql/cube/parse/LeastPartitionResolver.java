package org.apache.hadoop.hive.ql.cube.parse;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


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
          cubeql.addFactPruningMsgs(fact.fact, fact + " is not considered" +
            " as it requires more partitions to be queried");
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
