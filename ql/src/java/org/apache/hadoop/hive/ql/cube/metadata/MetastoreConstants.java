package org.apache.hadoop.hive.ql.cube.metadata;

public interface MetastoreConstants {
  public static final String TABLE_TYPE_KEY = "cube.table.type";
  public static final String CUBE_TABLE_PFX = "cube.table.";
  public static final String WEIGHT_KEY_SFX = ".weight";

  // Cube constants
  public static final String CUBE_KEY_PFX = "cube.";
  public static final String MEASURES_LIST_SFX = ".measures.list";
  public static final String DIMENSIONS_LIST_SFX = ".dimensions.list";
  public static final String TIMED_DIMENSIONS_LIST_SFX = ".timed.dimensions.list";

  // fact constants
  public static final String FACT_KEY_PFX = "cube.fact.";
  public static final String UPDATE_PERIOD_SFX = ".updateperiods";
  public static final String CUBE_NAME_SFX = ".cubename";
  public static final String VALID_COLUMNS_SFX = ".valid.columns";

  // column constants
  public static final String COL_PFX = "cube.col.";
  public static final String TYPE_SFX = ".type";
  public static final String START_TIME_SFX = ".starttime";
  public static final String END_TIME_SFX = ".endtime";
  public static final String COST_SFX = ".cost";

  // measure constants
  public static final String MEASURE_KEY_PFX = "cube.measure.";
  public static final String UNIT_SFX = ".unit";
  public static final String AGGR_SFX = ".aggregate";
  public static final String EXPR_SFX = ".expr";
  public static final String FORMATSTRING_SFX = ".format";

  // dimension constants
  public static final String DIM_KEY_PFX = "cube.dimension.";
  public static final String DIM_REFERS_SFX = ".refers";
  public static final String TABLE_COLUMN_SEPERATOR = ".";
  public static final String INLINE_SIZE_SFX = ".inline.size";
  public static final String INLINE_VALUES_SFX = ".inline.values";
  public static final String HIERARCHY_SFX = ".hierarchy.";
  public static final String CLASS_SFX = ".class";
  public static final String DUMP_PERIOD_SFX = ".dumpperiod";
  public static final String STORAGE_LIST_SFX = ".storages";
}
