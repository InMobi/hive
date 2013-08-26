package org.apache.hadoop.hive.ql.cube.parse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestStorageTableResolver {

  @Test
  public void testMinimalAnsweringTables() {
    Set<String> s1 = new HashSet<String>();
    s1.add("S1");
    Set<String> s2 = new HashSet<String>();
    s2.add("S2");
    Set<String> s3 = new HashSet<String>();
    s3.add("S3");
    Set<String> s4 = new HashSet<String>();
    s4.add("S4");
    Set<String> s5 = new HashSet<String>();
    s5.add("S5");
    Set<String> s123 = new HashSet<String>();
    s123.addAll(s1);
    s123.addAll(s2);
    s123.addAll(s3);

    Set<String> s12 = new HashSet<String>();
    s12.addAll(s1);
    s12.addAll(s2);
    Set<String> s23 = new HashSet<String>();
    s23.addAll(s2);
    s23.addAll(s3);
    Set<String> s24 = new HashSet<String>();
    s24.addAll(s2);
    s24.addAll(s4);
    Set<String> s34 = new HashSet<String>();
    s34.addAll(s3);
    s34.addAll(s4);

    Configuration conf = new Configuration();
    // {s1,s2,s3}, {s3}, {s3} -> {s3}
    Map<String, Set<String>> answeringTablesMap =
        new HashMap<String, Set<String>>();
    answeringTablesMap.put("month", s123);
    answeringTablesMap.put("day", s3);
    answeringTablesMap.put("hour", s3);
    StorageTableResolver str = new StorageTableResolver(conf);
    Map<String, Set<String>> result = new HashMap<String, Set<String>>();
    boolean mts = str.getMinimalAnsweringTables(answeringTablesMap, result);
    System.out.println("results:" + result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("S3", result.keySet().iterator().next());
    Set<String> coveredParts = result.get("S3");
    Assert.assertEquals(3, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("month"));
    Assert.assertTrue(coveredParts.contains("day"));
    Assert.assertTrue(coveredParts.contains("hour"));
    Assert.assertTrue(mts);

    // {s1,s2,s3}, {s4}, {s5} - > {s1,s4,s5} or {s2,s4,s5} or {s3,s4,s5}
    answeringTablesMap =
        new HashMap<String, Set<String>>();
    answeringTablesMap.put("month", s123);
    answeringTablesMap.put("day", s4);
    answeringTablesMap.put("hour", s5);
    str = new StorageTableResolver(conf);
    result = new HashMap<String, Set<String>>();
    mts = str.getMinimalAnsweringTables(answeringTablesMap, result);
    System.out.println("results:" + result);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.keySet().contains("S4"));
    Assert.assertTrue(result.keySet().contains("S5"));
    Assert.assertTrue(result.keySet().contains("S1") ||
        result.keySet().contains("S2") || result.keySet().contains("S3"));
    coveredParts = result.get("S4");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("day"));
    coveredParts = result.get("S5");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("hour"));
    coveredParts = result.get("S1");
    if (coveredParts == null) {
      coveredParts = result.get("S2");
    }
    if (coveredParts == null) {
      coveredParts = result.get("S3");
    }
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("month"));
    Assert.assertTrue(mts);

    // {s1}, {s2}, {s3} -> {s1,s2,s3}
    answeringTablesMap =
        new HashMap<String, Set<String>>();
    answeringTablesMap.put("month", s1);
    answeringTablesMap.put("day", s2);
    answeringTablesMap.put("hour", s3);
    str = new StorageTableResolver(conf);
    result = new HashMap<String, Set<String>>();
    mts = str.getMinimalAnsweringTables(answeringTablesMap, result);
    System.out.println("results:" + result);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S3"));
    coveredParts = result.get("S1");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("month"));
    coveredParts = result.get("S2");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("day"));
    coveredParts = result.get("S3");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("hour"));
    Assert.assertTrue(mts);

    // {s1, s2}, {s2, s3}, {s4} -> {s2,s4}
    answeringTablesMap =
        new HashMap<String, Set<String>>();
    answeringTablesMap.put("month", s12);
    answeringTablesMap.put("day", s23);
    answeringTablesMap.put("hour", s4);
    str = new StorageTableResolver(conf);
    result = new HashMap<String, Set<String>>();
    mts = str.getMinimalAnsweringTables(answeringTablesMap, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S4"));
    coveredParts = result.get("S2");
    Assert.assertEquals(2, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("month"));
    Assert.assertTrue(coveredParts.contains("day"));
    coveredParts = result.get("S4");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(coveredParts.contains("hour"));
    Assert.assertTrue(mts);

    // {s1, s2}, {s2, s4}, {s4} -> {s1,s4} or {s2,s4}
    answeringTablesMap =
        new HashMap<String, Set<String>>();
    answeringTablesMap.put("month", s12);
    answeringTablesMap.put("day", s24);
    answeringTablesMap.put("hour", s4);
    str = new StorageTableResolver(conf);
    result = new HashMap<String, Set<String>>();
    mts = str.getMinimalAnsweringTables(answeringTablesMap, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2") || result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S4"));
    coveredParts = result.get("S1");
    if (coveredParts == null) {
      coveredParts = result.get("S2");
      Assert.assertTrue(coveredParts.size() >= 1);
      Assert.assertTrue(coveredParts.contains("month"));
      if (coveredParts.size() == 2) {
        Assert.assertTrue(coveredParts.contains("day"));
        Assert.assertEquals(1, result.get("S4").size());
      }
      coveredParts = result.get("S4");
      Assert.assertTrue(coveredParts.size() >= 1);
      Assert.assertTrue(coveredParts.contains("hour"));
      if (coveredParts.size() == 2) {
        Assert.assertTrue(coveredParts.contains("day"));
        Assert.assertEquals(1, result.get("S2").size());
      }
    } else {
      Assert.assertEquals(1, coveredParts.size());
      Assert.assertTrue(coveredParts.contains("month"));
      coveredParts = result.get("S4");
      Assert.assertTrue(coveredParts.size() >= 1);
      Assert.assertTrue(coveredParts.contains("hour"));
      Assert.assertTrue(coveredParts.contains("day"));
    }
    Assert.assertFalse(mts);

    // {s1, s2}, {s2, s3}, {s3,s4} -> {s2,s3}
    answeringTablesMap =
        new HashMap<String, Set<String>>();
    answeringTablesMap.put("month", s12);
    answeringTablesMap.put("day", s23);
    answeringTablesMap.put("hour", s34);
    str = new StorageTableResolver(conf);
    result = new HashMap<String, Set<String>>();
    mts = str.getMinimalAnsweringTables(answeringTablesMap, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S3"));
    coveredParts = result.get("S2");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(coveredParts.contains("month"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(coveredParts.contains("day"));
      Assert.assertEquals(1, result.get("S3").size());
    }
    coveredParts = result.get("S3");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(coveredParts.contains("hour"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(coveredParts.contains("day"));
      Assert.assertEquals(1, result.get("S2").size());
    }
    Assert.assertFalse(mts);

    // {s1, s2}, {s2}, {s1} -> {s1,s2}
    answeringTablesMap =
        new HashMap<String, Set<String>>();
    answeringTablesMap.put("month", s12);
    answeringTablesMap.put("day", s2);
    answeringTablesMap.put("hour", s1);
    str = new StorageTableResolver(conf);
    result = new HashMap<String, Set<String>>();
    mts = str.getMinimalAnsweringTables(answeringTablesMap, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S2"));
    coveredParts = result.get("S2");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(coveredParts.contains("day"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(coveredParts.contains("month"));
      Assert.assertEquals(1, result.get("S1").size());
    }
    coveredParts = result.get("S1");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(coveredParts.contains("hour"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(coveredParts.contains("month"));
      Assert.assertEquals(1, result.get("S2").size());
    }
    Assert.assertFalse(mts);
  }
}
