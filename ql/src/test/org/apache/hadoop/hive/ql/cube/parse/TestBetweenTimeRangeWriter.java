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

import java.text.DateFormat;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;

public class TestBetweenTimeRangeWriter extends TestTimeRangeWriter {

  @Override
  public TimeRangeWriter getTimerangeWriter() {
    return new BetweenTimeRangeWriter();
  }

  @Override
  public boolean failDisjoint() {
    return true;
  }

  @Override
  public void validateDisjoint(String whereClause, DateFormat format) {
    Assert.fail();
  }

  @Override
  public void validateConsecutive(String whereClause, DateFormat format) {

    String first = null;
    String last = null;
    if (format == null) {
      first = UpdatePeriod.DAILY.format().format(CubeTestSetup.twodaysBack);
      last = UpdatePeriod.DAILY.format().format(CubeTestSetup.now);

    } else {
      first = format.format(CubeTestSetup.twodaysBack);
      last = format.format(CubeTestSetup.now);
    }
    String expected = " (test.dt BETWEEN " + first + " AND " + last + ") ";
    Assert.assertEquals(expected, whereClause);
  }

}
