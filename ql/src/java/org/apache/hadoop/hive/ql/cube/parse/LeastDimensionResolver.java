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
