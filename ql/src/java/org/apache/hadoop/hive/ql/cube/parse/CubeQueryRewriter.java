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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class CubeQueryRewriter {
  private final Configuration conf;
  private final List<ContextRewriter> rewriters = new ArrayList<ContextRewriter>();
  private final HiveConf hconf;
  private final Context ctx;

  public CubeQueryRewriter(Configuration conf) throws SemanticException {
    this.conf = conf;
    hconf = new HiveConf(conf, HiveConf.class);
    try {
      ctx = new Context(hconf);
    } catch (IOException e) {
      throw new SemanticException("Error creating ql context", e);
    }
    setupRewriters();
  }

  private void setupRewriters() {
    // Rewrite base trees (groupby, having, orderby, limit) using aliases
    rewriters.add(new AliasReplacer(conf));
    // Resolve aggregations and generate base select tree
    rewriters.add(new AggregateResolver(conf));
    rewriters.add(new GroupbyResolver(conf));
    // Resolve joins and generate base join tree
    rewriters.add(new JoinResolver(conf));
    // Resolve storage partitions and table names
    rewriters.add(new StorageTableResolver(conf));
    rewriters.add(new LeastPartitionResolver(conf));
    rewriters.add(new LightestFactResolver(conf));
    rewriters.add(new LeastDimensionResolver(conf));
  }

  public CubeQueryContext rewrite(ASTNode astnode) throws SemanticException {
    CubeSemanticAnalyzer analyzer = new CubeSemanticAnalyzer(hconf);
    analyzer.analyze(astnode, ctx);
    CubeQueryContext ctx = analyzer.getQueryContext();
    rewrite(rewriters, ctx);
    return ctx;
  }

  public CubeQueryContext rewrite(String command)
      throws ParseException,SemanticException {
    if (command != null) {
      command = command.replace("\n", "");
    }
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command, null);
    tree = ParseUtils.findRootNonNullToken(tree);
    return rewrite(tree);
  }

  private void rewrite(List<ContextRewriter> rewriters, CubeQueryContext ctx)
      throws SemanticException {
    for (ContextRewriter rewriter : rewriters) {
      rewriter.rewriteContext(ctx);
    }
  }

  public Context getQLContext() {
    return ctx;
  }
}
