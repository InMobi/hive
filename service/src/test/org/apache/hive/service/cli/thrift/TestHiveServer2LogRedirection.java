package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIServiceTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.util.UUID;

public class TestHiveServer2LogRedirection extends CLIServiceTest {

  protected static ThriftCLIService service;
  private static File logDir;
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HiveConf conf = new HiveConf();

    String tmpLogDirLoc = UUID.randomUUID().toString();
    logDir = new File(new File("/tmp"), tmpLogDirLoc);
    logDir.mkdir();
    System.out.println("log dir - " + logDir);

    service = new EmbeddedThriftBinaryCLIService(conf);
    client = new ThriftCLIServiceClient(service);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceTest#setUp()
   */
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.CLIServiceTest#tearDown()
   */
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
}
