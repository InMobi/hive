package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestStorageUtil {

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
    List<FactPartition> answeringParts =
        new ArrayList<FactPartition>();
    answeringParts.add(new FactPartition("dt", "month", null, null, s123));
    answeringParts.add(new FactPartition("dt", "day", null, null, s3));
    answeringParts.add(new FactPartition("dt", "hour", null, null, s3));
    Map<String, Set<FactPartition>> result = new HashMap<String, Set<FactPartition>>();
    boolean mts = StorageUtil.getMinimalAnsweringTables(answeringParts, result);
    System.out.println("results:" + result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("S3", result.keySet().iterator().next());
    Set<FactPartition> coveredParts = result.get("S3");
    Assert.assertEquals(3, coveredParts.size());
    Assert.assertTrue(contains(coveredParts, "month"));
    Assert.assertTrue(contains(coveredParts, "day"));
    Assert.assertTrue(contains(coveredParts, "hour"));
    Assert.assertTrue(mts);

    // {s1,s2,s3}, {s4}, {s5} - > {s1,s4,s5} or {s2,s4,s5} or {s3,s4,s5}
    answeringParts = new ArrayList<FactPartition>();
    answeringParts.add(new FactPartition("dt", "month", null, null, s123));
    answeringParts.add(new FactPartition("dt", "day", null, null, s4));
    answeringParts.add(new FactPartition("dt", "hour", null, null, s5));
    result = new HashMap<String, Set<FactPartition>>();
    mts = StorageUtil.getMinimalAnsweringTables(answeringParts, result);
    System.out.println("results:" + result);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.keySet().contains("S4"));
    Assert.assertTrue(result.keySet().contains("S5"));
    Assert.assertTrue(result.keySet().contains("S1") ||
        result.keySet().contains("S2") || result.keySet().contains("S3"));
    coveredParts = result.get("S4");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"day"));
    coveredParts = result.get("S5");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"hour"));
    coveredParts = result.get("S1");
    if (coveredParts == null) {
      coveredParts = result.get("S2");
    }
    if (coveredParts == null) {
      coveredParts = result.get("S3");
    }
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"month"));
    Assert.assertTrue(mts);

    // {s1}, {s2}, {s3} -> {s1,s2,s3}
    answeringParts = new ArrayList<FactPartition>();
    answeringParts.add(new FactPartition("dt", "month", null, null, s1));
    answeringParts.add(new FactPartition("dt", "day", null, null, s2));
    answeringParts.add(new FactPartition("dt", "hour", null, null, s3));
    result = new HashMap<String, Set<FactPartition>>();
    mts = StorageUtil.getMinimalAnsweringTables(answeringParts, result);
    System.out.println("results:" + result);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S3"));
    coveredParts = result.get("S1");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"month"));
    coveredParts = result.get("S2");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"day"));
    coveredParts = result.get("S3");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"hour"));
    Assert.assertTrue(mts);

    // {s1, s2}, {s2, s3}, {s4} -> {s2,s4}
    answeringParts = new ArrayList<FactPartition>();
    answeringParts.add(new FactPartition("dt", "month", null, null, s12));
    answeringParts.add(new FactPartition("dt", "day", null, null, s23));
    answeringParts.add(new FactPartition("dt", "hour", null, null, s4));
    result = new HashMap<String, Set<FactPartition>>();
    mts = StorageUtil.getMinimalAnsweringTables(answeringParts, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S4"));
    coveredParts = result.get("S2");
    Assert.assertEquals(2, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"month"));
    Assert.assertTrue(contains(coveredParts,"day"));
    coveredParts = result.get("S4");
    Assert.assertEquals(1, coveredParts.size());
    Assert.assertTrue(contains(coveredParts,"hour"));
    Assert.assertTrue(mts);

    // {s1, s2}, {s2, s4}, {s4} -> {s1,s4} or {s2,s4}
    answeringParts = new ArrayList<FactPartition>();
    answeringParts.add(new FactPartition("dt", "month", null, null, s12));
    answeringParts.add(new FactPartition("dt", "day", null, null, s24));
    answeringParts.add(new FactPartition("dt", "hour", null, null, s4));
    result = new HashMap<String, Set<FactPartition>>();
    mts = StorageUtil.getMinimalAnsweringTables(answeringParts, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2") || result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S4"));
    coveredParts = result.get("S1");
    if (coveredParts == null) {
      coveredParts = result.get("S2");
      Assert.assertTrue(coveredParts.size() >= 1);
      Assert.assertTrue(contains(coveredParts,"month"));
      if (coveredParts.size() == 2) {
        Assert.assertTrue(contains(coveredParts,"day"));
        Assert.assertEquals(1, result.get("S4").size());
      }
      coveredParts = result.get("S4");
      Assert.assertTrue(coveredParts.size() >= 1);
      Assert.assertTrue(contains(coveredParts,"hour"));
      if (coveredParts.size() == 2) {
        Assert.assertTrue(contains(coveredParts,"day"));
        Assert.assertEquals(1, result.get("S2").size());
      }
    } else {
      Assert.assertEquals(1, coveredParts.size());
      Assert.assertTrue(contains(coveredParts,"month"));
      coveredParts = result.get("S4");
      Assert.assertTrue(coveredParts.size() >= 1);
      Assert.assertTrue(contains(coveredParts,"hour"));
      Assert.assertTrue(contains(coveredParts,"day"));
    }
    Assert.assertFalse(mts);

    // {s1, s2}, {s2, s3}, {s3,s4} -> {s2,s3}
    answeringParts = new ArrayList<FactPartition>();
    answeringParts.add(new FactPartition("dt", "month", null, null, s12));
    answeringParts.add(new FactPartition("dt", "day", null, null, s23));
    answeringParts.add(new FactPartition("dt", "hour", null, null, s34));
    result = new HashMap<String, Set<FactPartition>>();
    mts = StorageUtil.getMinimalAnsweringTables(answeringParts, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S2"));
    Assert.assertTrue(result.keySet().contains("S3"));
    coveredParts = result.get("S2");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(contains(coveredParts,"month"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(contains(coveredParts,"day"));
      Assert.assertEquals(1, result.get("S3").size());
    }
    coveredParts = result.get("S3");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(contains(coveredParts,"hour"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(contains(coveredParts,"day"));
      Assert.assertEquals(1, result.get("S2").size());
    }
    Assert.assertFalse(mts);

    // {s1, s2}, {s2}, {s1} -> {s1,s2}
    answeringParts = new ArrayList<FactPartition>();
    answeringParts.add(new FactPartition("dt", "month", null, null, s12));
    answeringParts.add(new FactPartition("dt", "day", null, null, s2));
    answeringParts.add(new FactPartition("dt", "hour", null, null, s1));
    result = new HashMap<String, Set<FactPartition>>();
    mts = StorageUtil.getMinimalAnsweringTables(answeringParts, result);
    System.out.println("results:" + result);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.keySet().contains("S1"));
    Assert.assertTrue(result.keySet().contains("S2"));
    coveredParts = result.get("S2");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(contains(coveredParts,"day"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(contains(coveredParts,"month"));
      Assert.assertEquals(1, result.get("S1").size());
    }
    coveredParts = result.get("S1");
    Assert.assertTrue(coveredParts.size() >= 1);
    Assert.assertTrue(contains(coveredParts,"hour"));
    if (coveredParts.size() == 2) {
      Assert.assertTrue(contains(coveredParts,"month"));
      Assert.assertEquals(1, result.get("S2").size());
    }
    Assert.assertFalse(mts);
  }

  private boolean contains(Set<FactPartition> parts, String partSpec) {
    for (FactPartition part : parts) {
      if (part.partSpec.equals(partSpec)) {
        return true;
      }
    }
    return false;
  }
}
