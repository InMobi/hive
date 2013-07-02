package org.apache.hadoop.hive.ql.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_GROUPBY;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_INSERT;
import junit.framework.Assert;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.junit.Test;

public class TestHQLParser {
  @Test
  public void testGroupByOrderByGetString() throws Exception {
    String query = "SELECT a,b, sum(c) FROM tab GROUP BY a,f(b), d+e ORDER BY a, g(b), e/100";
    ASTNode node = HQLParser.parseHQL(query);
    HQLParser.printAST(node);

    ASTNode groupby = HQLParser.findNodeByPath(node, TOK_INSERT, TOK_GROUPBY);
    String expected = "a , f( b ), ( d  +  e )";
    Assert.assertEquals(expected, HQLParser.getString(groupby).trim());

    ASTNode orderby = HQLParser.findNodeByPath(node, TOK_INSERT, HiveParser.TOK_ORDERBY);
    String expectedOrderBy = "a , g( b ), ( e  /  100 )";
    Assert.assertEquals(expectedOrderBy, HQLParser.getString(orderby).trim());

  }

}
