package org.apache.hadoop.hive.ql.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.*;
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


  @Test
  public void testLiteralCaseIsPreserved() throws Exception {
    String literalQuery = "SELECT 'abc' AS col1, 'DEF' AS col2 FROM foo where col3='GHI' "
        + "AND col4 = 'JKLmno'";

    ASTNode tree = HQLParser.parseHQL(literalQuery);

    ASTNode select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select).trim();
    String expectedSelect = "'abc'  col1 ,  'DEF'  col2";
    Assert.assertEquals(expectedSelect, selectStr);

    ASTNode where = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where).trim();
    String expectedWhere = "(( col3  =  'GHI' ) and ( col4  =  'JKLmno' ))";
    Assert.assertEquals(expectedWhere, whereStr);
  }

  @Test
  public void testCaseStatementGetString() throws Exception {
    String query = "SELECT  "
        + "CASE (col1 * 100)/200 + 5 "
        + "WHEN 'ABC' THEN 'def' "
        + "WHEN 'EFG' THEN 'hij' "
        + "ELSE 'XyZ' "
        + "END AS ComplexCaseStatement FROM FOO";

    ASTNode tree = HQLParser.parseHQL(query);
    ASTNode select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    String selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause ");
    System.out.println(selectStr);
    Assert.assertEquals("case ((( col1  *  100 ) /  200 ) +  5 )"
        + " when  'ABC'  then  'def'  when  'EFG'  then  'hij'  else  'XyZ'  "
        + "end  complexcasestatement", selectStr.trim());

    String q2 = "SELECT "
        + "CASE WHEN col1 = 'abc' then 'def' "
        + "when col1 = 'ghi' then 'jkl' "
        + "else 'none' END AS Complex_Case_Statement_2"
        + " from FOO";

    tree = HQLParser.parseHQL(q2);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause 2");
    System.out.println(selectStr);
    Assert.assertEquals("case  "
        + "when ( col1  =  'abc' ) then  'def'  "
        + "when ( col1  =  'ghi' ) then  'jkl'  "
        + "else  'none'  end  complex_case_statement_2", selectStr.trim());


    String q3 = "SELECT  "
        + "CASE (col1 * 100)/200 + 5 "
        + "WHEN 'ABC' THEN 'def' "
        + "WHEN 'EFG' THEN 'hij' "
        + "END AS ComplexCaseStatement FROM FOO";

    tree = HQLParser.parseHQL(q3);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause ");
    System.out.println(selectStr);
    Assert.assertEquals("case ((( col1  *  100 ) /  200 ) +  5 ) "
        + "when  'ABC'  then  'def'  "
        + "when  'EFG'  then  'hij'  "
        + "end  complexcasestatement",
        selectStr.trim());
    
    
    String q4 = "SELECT "
        + "CASE WHEN col1 = 'abc' then 'def' "
        + "when col1 = 'ghi' then 'jkl' "
        + "END AS Complex_Case_Statement_2"
        + " from FOO";

    tree = HQLParser.parseHQL(q4);
    select = HQLParser.findNodeByPath(tree, TOK_INSERT, TOK_SELECT);
    selectStr = HQLParser.getString(select);
    System.out.println("reconstructed clause 2");
    System.out.println(selectStr);
    Assert.assertEquals("case  "
        + "when ( col1  =  'abc' ) then  'def'  "
        + "when ( col1  =  'ghi' ) then  'jkl' "
        + " end  complex_case_statement_2", selectStr.trim());

  }
  
  @Test
  public void testIsNullCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 IS NULL";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1  is null", whereStr.trim());
  }
  
  
  @Test
  public void testIsNotNullCondition() throws Exception {
    String q1 = "SELECT * FROM FOO WHERE col1 IS NOT NULL";
    ASTNode where = HQLParser.findNodeByPath(HQLParser.parseHQL(q1), TOK_INSERT, TOK_WHERE);
    String whereStr = HQLParser.getString(where);
    System.out.println(whereStr);
    Assert.assertEquals("col1  is not null", whereStr.trim());
  }

}
