package org.apache.hadoop.hive.ql.cube.parse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
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
    Map<UpdatePeriod, Set<String>> answeringTablesMap =
        new HashMap<UpdatePeriod, Set<String>>();
    answeringTablesMap.put(UpdatePeriod.MONTHLY, s123);
    answeringTablesMap.put(UpdatePeriod.DAILY, s3);
    answeringTablesMap.put(UpdatePeriod.HOURLY, s3);
    StorageTableResolver str = new StorageTableResolver(conf);
    Map<String, Set<UpdatePeriod>> result = str
        .getMinimalAnsweringTables(answeringTablesMap);
    System.out.println("results:" + result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("S3", result.keySet().iterator().next());
    Set<UpdatePeriod> coveredPeriods = result.get("S3");
    Assert.assertEquals(3, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
    Assert.assertTrue(str.enabledMultiTableSelect());

    // {s1,s2,s3}, {s4}, {s5} - > {s1,s4,s5} or {s2,s4,s5} or {s3,s4,s5}
    answeringTablesMap =
        new HashMap<UpdatePeriod, Set<String>>();
    answeringTablesMap.put(UpdatePeriod.MONTHLY, s123);
    answeringTablesMap.put(UpdatePeriod.DAILY, s4);
    answeringTablesMap.put(UpdatePeriod.HOURLY, s5);
    str = new StorageTableResolver(conf);
    result = str.getMinimalAnsweringTables(answeringTablesMap);
    System.out.println("results:" + result);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.keySet().contains("S4"));
    Assert.assertTrue(result.keySet().contains("S5"));
    Assert.assertTrue(result.keySet().contains("S1") ||
        result.keySet().contains("S2") || result.keySet().contains("S3"));
    coveredPeriods = result.get("S4");
    Assert.assertEquals(1, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
    coveredPeriods = result.get("S5");
    Assert.assertEquals(1, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
    coveredPeriods = result.get("S1");
    if (coveredPeriods == null) {
      coveredPeriods = result.get("S2");
    }
    if (coveredPeriods == null) {
      coveredPeriods = result.get("S3");
    }
    Assert.assertEquals(1, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
    Assert.assertTrue(str.enabledMultiTableSelect());

    // {s1}, {s2}, {s3} -> {s1,s2,s3}
    answeringTablesMap =
        new HashMap<UpdatePeriod, Set<String>>();
    answeringTablesMap.put(UpdatePeriod.MONTHLY, s1);
    answeringTablesMap.put(UpdatePeriod.DAILY, s2);
    answeringTablesMap.put(UpdatePeriod.HOURLY, s3);
    str = new StorageTableResolver(conf);
    result = str.getMinimalAnsweringTables(answeringTablesMap);
    System.out.println("results:" + result);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S3"));
    coveredPeriods = result.get("S1");
    Assert.assertEquals(1, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
    coveredPeriods = result.get("S2");
    Assert.assertEquals(1, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
    coveredPeriods = result.get("S3");
    Assert.assertEquals(1, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
    Assert.assertTrue(str.enabledMultiTableSelect());

    // {s1, s2}, {s2, s3}, {s4} -> {s2,s4}
    answeringTablesMap =
        new HashMap<UpdatePeriod, Set<String>>();
    answeringTablesMap.put(UpdatePeriod.MONTHLY, s12);
    answeringTablesMap.put(UpdatePeriod.DAILY, s23);
    answeringTablesMap.put(UpdatePeriod.HOURLY, s4);
    str = new StorageTableResolver(conf);
    result = str.getMinimalAnsweringTables(answeringTablesMap);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S4"));
    coveredPeriods = result.get("S2");
    Assert.assertEquals(2, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
    coveredPeriods = result.get("S4");
    Assert.assertEquals(1, coveredPeriods.size());
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
    Assert.assertTrue(str.enabledMultiTableSelect());

    // {s1, s2}, {s2, s4}, {s4} -> {s1,s4} or {s2,s4}
    answeringTablesMap =
        new HashMap<UpdatePeriod, Set<String>>();
    answeringTablesMap.put(UpdatePeriod.MONTHLY, s12);
    answeringTablesMap.put(UpdatePeriod.DAILY, s24);
    answeringTablesMap.put(UpdatePeriod.HOURLY, s4);
    str = new StorageTableResolver(conf);
    result = str.getMinimalAnsweringTables(answeringTablesMap);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2") || result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S4"));
    coveredPeriods = result.get("S1");
    if (coveredPeriods == null) {
      coveredPeriods = result.get("S2");
      Assert.assertTrue(coveredPeriods.size() >= 1);
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
      if (coveredPeriods.size() == 2) {
        Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
        Assert.assertEquals(1, result.get("S4").size());
      }
      coveredPeriods = result.get("S4");
      Assert.assertTrue(coveredPeriods.size() >= 1);
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
      if (coveredPeriods.size() == 2) {
        Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
        Assert.assertEquals(1, result.get("S2").size());
      }
    } else {
      Assert.assertEquals(1, coveredPeriods.size());
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
      coveredPeriods = result.get("S4");
      Assert.assertTrue(coveredPeriods.size() >= 1);
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
    }
    Assert.assertFalse(str.enabledMultiTableSelect());

    // {s1, s2}, {s2, s3}, {s3,s4} -> {s2,s3}
    answeringTablesMap =
        new HashMap<UpdatePeriod, Set<String>>();
    answeringTablesMap.put(UpdatePeriod.MONTHLY, s12);
    answeringTablesMap.put(UpdatePeriod.DAILY, s23);
    answeringTablesMap.put(UpdatePeriod.HOURLY, s34);
    str = new StorageTableResolver(conf);
    result = str.getMinimalAnsweringTables(answeringTablesMap);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S3"));
    coveredPeriods = result.get("S2");
    Assert.assertTrue(coveredPeriods.size() >= 1);
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
    if (coveredPeriods.size() == 2) {
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
      Assert.assertEquals(1, result.get("S3").size());
    }
    coveredPeriods = result.get("S3");
    Assert.assertTrue(coveredPeriods.size() >= 1);
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
    if (coveredPeriods.size() == 2) {
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
      Assert.assertEquals(1, result.get("S2").size());
    }
    Assert.assertFalse(str.enabledMultiTableSelect());

    // {s1, s2}, {s2}, {s1} -> {s1,s2}
    answeringTablesMap =
        new HashMap<UpdatePeriod, Set<String>>();
    answeringTablesMap.put(UpdatePeriod.MONTHLY, s12);
    answeringTablesMap.put(UpdatePeriod.DAILY, s2);
    answeringTablesMap.put(UpdatePeriod.HOURLY, s1);
    str = new StorageTableResolver(conf);
    result = str.getMinimalAnsweringTables(answeringTablesMap);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S2"));
    coveredPeriods = result.get("S2");
    Assert.assertTrue(coveredPeriods.size() >= 1);
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.DAILY));
    if (coveredPeriods.size() == 2) {
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
      Assert.assertEquals(1, result.get("S1").size());
    }
    coveredPeriods = result.get("S1");
    Assert.assertTrue(coveredPeriods.size() >= 1);
    Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.HOURLY));
    if (coveredPeriods.size() == 2) {
      Assert.assertTrue(coveredPeriods.contains(UpdatePeriod.MONTHLY));
      Assert.assertEquals(1, result.get("S2").size());
    }
    Assert.assertFalse(str.enabledMultiTableSelect());
  }
}
