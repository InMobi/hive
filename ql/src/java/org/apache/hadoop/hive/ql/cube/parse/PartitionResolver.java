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


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class PartitionResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(
      PartitionResolver.class.getName());

  public PartitionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    /*if (!cubeql.getCandidateFactTables().isEmpty()) {
      Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factPartitionMap =
          new HashMap<CubeFactTable, Map<UpdatePeriod, List<String>>>();
      Date fromDate = cubeql.getFromDate();
      Date toDate = cubeql.getToDate();

      Calendar cal = Calendar.getInstance();
      cal.setTime(fromDate);
      for (Iterator<CubeFactTable> i = cubeql.getCandidateFactTables()
          .iterator(); i.hasNext();) {
        CubeFactTable fact = i.next();
        Map<UpdatePeriod, List<String>> partitionColMap =
            new HashMap<UpdatePeriod, List<String>>();
        if (!getPartitions(fact, fromDate, toDate, partitionColMap)) {
          i.remove();
        } else {
          factPartitionMap.put(fact, partitionColMap);
        }
      }
      // set partition cols map in cubeql
      cubeql.setFactPartitionMap(factPartitionMap);
    } */
  }

  private boolean getPartitions(CubeFactTable fact, Date fromDate, Date toDate,
      Map<UpdatePeriod, List<String>> partitionColMap)
      throws SemanticException {
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return true;
    }

    UpdatePeriod interval = fact.maxIntervalInRange(fromDate, toDate, null);
    if (interval == null) {
      LOG.info("Could not find partition for given range:"
          + fromDate + "-" + toDate + " in fact:" + fact.getName());
      return false;
    }

    Date ceilFromDate = DateUtil.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtil.getFloorDate(toDate, interval);
    List<String> partitions = fact.getPartitions(ceilFromDate, floorToDate,
        interval);
    if (partitions != null) {
      List<String> parts = partitionColMap.get(interval);
      if (parts == null) {
        parts = new ArrayList<String>();
        partitionColMap.put(interval, parts);
      }
      parts.addAll(partitions);
    }
    return (getPartitions(fact, fromDate, ceilFromDate, partitionColMap)
    && getPartitions(fact, floorToDate, toDate, partitionColMap));
  }
}
