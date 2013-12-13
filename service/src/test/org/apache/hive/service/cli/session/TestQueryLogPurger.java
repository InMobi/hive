package org.apache.hive.service.cli.session;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class TestQueryLogPurger {
  @Test
  public void testQueryLogPurger() throws Exception {
    // Create a session dir and few log files inside it
    File queryLogDir = new File(FileUtils.getTempDirectory(), "query_log_dir");
    try {
      queryLogDir.mkdir();
      final int PURGE_PERIOD = 2000;
      SessionManager.QueryLogPurger purger = new SessionManager.QueryLogPurger(queryLogDir, PURGE_PERIOD);
      System.out.println("Creating test files " + new Date());
      // Create a session with closed marker
      createSessionFiles(queryLogDir, 1, true);
      // Another session without closed marker
      createSessionFiles(queryLogDir, 2, false);

      System.out.println("Done creating test files " + new Date());
      long before = System.currentTimeMillis();
      System.out.println("purger run 1 " + new Date());
      purger.run();
      if (System.currentTimeMillis() - before < PURGE_PERIOD) {
        System.out.println("Should not delete in this run " + new Date());
        // Expecting logs not to be deleted in this run
        assertEquals(2, queryLogDir.listFiles().length);
      }

      Thread.sleep(PURGE_PERIOD);
      System.out.println("purger run 2 " + new Date());
      purger.run();
      // Now they should be deleted since age > 150
      assertEquals(0, queryLogDir.listFiles().length);
    }
    finally {
      FileUtils.forceDelete(queryLogDir);
      System.out.println("deleted test files");
    }
  }

  private void createSessionFiles(File queryLogDir, int sessionId, boolean closeSession) throws Exception {
    String session = "test_session" + sessionId;
    String testOp = "op1";
    File sessionDir = new File(queryLogDir, session);
    sessionDir.mkdir();

    File sessionOut = new File(sessionDir, "session.out");
    sessionOut.createNewFile();
    File sessionErr = new File(sessionDir, "session.err");
    sessionErr.createNewFile();


    File op1Dir = new File(sessionDir, testOp);
    op1Dir.mkdir();

    File op1Out = new File(op1Dir, testOp +".out");
    op1Out.createNewFile();
    File op1Err = new File(op1Dir, testOp + ".err");
    op1Err.createNewFile();

    if (closeSession) {
      // Create session closed marker
      File sessionClosedMarker = new File(sessionDir, HiveSession.SESSION_CLOSED_MARKER);
      sessionClosedMarker.createNewFile();
    }
    System.out.println("Created session files " + sessionId);
  }
}
