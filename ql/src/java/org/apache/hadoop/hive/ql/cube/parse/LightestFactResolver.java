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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.parse.CandidateTablePruneCause.CubeTableCause;
import org.apache.hadoop.hive.ql.cube.parse.CubeQueryContext.CandidateFact;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LightestFactResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(
      LightestFactResolver.class.getName());

  public LightestFactResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    /*if (cubeql.getCube() != null && !cubeql.getCandidateFactTables()
        .isEmpty()) {
      Map<CandidateFact, Double> factWeightMap =
          new HashMap<CandidateFact, Double>();

      for (CandidateFact fact : cubeql.getCandidateFactTables()) {
        factWeightMap.put(fact, fact.fact.weight());
      }

      double minWeight = Collections.min(factWeightMap.values());

      for (Iterator<CandidateFact> i =
          cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CandidateFact fact = i.next();
        if (factWeightMap.get(fact) > minWeight) {
          LOG.info("Not considering fact:" + fact +
              " from candidate fact tables as it has more fact weight:"
              + factWeightMap.get(fact) + " minimum:"
              + minWeight);
          cubeql.addFactPruningMsgs(fact.fact, new CandidateTablePruneCause(
              fact.fact.getName(), CubeTableCause.MORE_WEIGHT));
          i.remove();
        }
      }
    }*/
    if (cubeql.getCube() != null && !cubeql.getCandidateFactSets()
        .isEmpty()) {
      Map<Set<CandidateFact>, Double> factWeightMap =
          new HashMap<Set<CandidateFact>, Double>();

      for (Set<CandidateFact> facts : cubeql.getCandidateFactSets()) {
        factWeightMap.put(facts, getWeitgh(facts));
      }

      double minWeight = Collections.min(factWeightMap.values());

      for (Iterator<Set<CandidateFact>> i =
          cubeql.getCandidateFactSets().iterator(); i.hasNext();) {
        Set<CandidateFact> facts = i.next();
        if (factWeightMap.get(facts) > minWeight) {
          LOG.info("Not considering facts:" + facts +
              " from candidate fact tables as it has more fact weight:"
              + factWeightMap.get(facts) + " minimum:"
              + minWeight);
          i.remove();
        }
      }
      cubeql.pruneCandidateFactWithCandidateSet(CubeTableCause.MORE_WEIGHT);
    }
  }

  private Double getWeitgh(Set<CandidateFact> set) {
    Double weight = 0.0;
    for (CandidateFact f :set) {
      weight += f.fact.weight();
    }
    return weight;
  }
}
