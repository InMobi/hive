package org.apache.hadoop.hive.ql.cube.metadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public abstract class CubeColumn implements Named {

  private final String name;
  private final Date startTime;
  private final Date endTime;
  private final Double cost;
  static SimpleDateFormat columnTimeFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

  public CubeColumn(String name, Date startTime, Date endTime, Double cost) {
    assert (name != null);
    this.name = name.toLowerCase();
    this.startTime = startTime;
    this.endTime = endTime;
    this.cost = cost;
  }

  private Date getDate(String propKey, Map<String, String> props) {
    String timeKey = props.get(propKey);
    if (timeKey != null) {
      try {
        return columnTimeFormat.parse(timeKey);
      } catch (Exception e) {
        // ignore and return null
      }
    }
    return null;
  }

  private Double getDouble(String propKey, Map<String, String> props) {
    String doubleStr = props.get(propKey);
    if (doubleStr != null) {
      try {
        return Double.parseDouble(doubleStr);
      } catch (Exception e) {
        // ignore and return null
      }
    }
    return null;
  }

  public CubeColumn(String name, Map<String, String> props) {
    this.name = name;
    this.startTime = getDate(MetastoreUtil.getCubeColStartTimePropertyKey(name),
        props);
    this.endTime = getDate(MetastoreUtil.getCubeColEndTimePropertyKey(name),
        props);
    this.cost = getDouble(MetastoreUtil.getCubeColCostPropertyKey(name),
        props);
  }

  public String getName() {
    return name;
  }

  /**
   * @return the startTime
   */
  public Date getStartTime() {
    return startTime;
  }

  /**
   * @return the endTime
   */
  public Date getEndTime() {
    return endTime;
  }

  /**
   * @return the cost
   */
  public Double getCost() {
    return cost;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(name);
    if (startTime != null) {
      builder.append("#start:");
      builder.append(columnTimeFormat.format(startTime));
    }
    if (endTime != null) {
      builder.append("#end:");
      builder.append(columnTimeFormat.format(endTime));
    }
    if (cost != null) {
      builder.append(":");
      builder.append(cost);
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getName() == null) ? 0 :
        getName().toLowerCase().hashCode());
    result = prime * result + ((getStartTime() == null) ? 0 :
      columnTimeFormat.format(getStartTime()).hashCode());
    result = prime * result + ((getEndTime() == null) ? 0 :
      columnTimeFormat.format(getEndTime()).hashCode());
    result = prime * result + ((getCost() == null) ? 0 :
      getCost().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CubeColumn other = (CubeColumn) obj;
    if (this.getName() == null) {
      if (other.getName() != null) {
        return false;
      }
    } else if (!this.getName().equalsIgnoreCase(other.getName())) {
      return false;
    }

    if (this.getStartTime() == null) {
      if (other.getStartTime() != null) {
        return false;
      }
    } else if (other.getStartTime() == null) {
      return false;
    } else if (!columnTimeFormat.format(this.getStartTime()).equals(
        columnTimeFormat.format(other.getStartTime()))) {
      return false;
    }

    if (this.getEndTime() == null) {
      if (other.getEndTime() != null) {
        return false;
      }
    } else if (other.getEndTime() == null) {
      return false;
    } else if (!columnTimeFormat.format(this.getEndTime()).equals(
        columnTimeFormat.format(other.getEndTime()))) {
      return false;
    }

    if (this.getCost() == null) {
      if (other.getCost() != null) {
        return false;
      }
    } else if (!this.getCost().equals(other.getCost())) {
      return false;
    }
    return true;
  }


  public void addProperties(Map<String, String> props) {
    if (startTime != null) {
      props.put(MetastoreUtil.getCubeColStartTimePropertyKey(getName()),
          columnTimeFormat.format(startTime));
    }
    if (endTime != null) {
      props.put(MetastoreUtil.getCubeColEndTimePropertyKey(getName()),
          columnTimeFormat.format(endTime));
    }
    if (cost != null) {
      props.put(MetastoreUtil.getCubeColCostPropertyKey(getName()),
          cost.toString());
    }
  }
}
