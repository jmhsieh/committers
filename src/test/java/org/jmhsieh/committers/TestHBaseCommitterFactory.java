package org.jmhsieh.committers;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests to validate the committer factory
 */
public class TestHBaseCommitterFactory {
  @Rule
  public TestName testName = new TestName();
  private final static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    util.startMiniCluster();
    System.err.println("===========");
  }

  @AfterClass
  public static void cleanup() throws Exception {
    System.err.println("===========");
    util.shutdownMiniCluster();
  }

  @Test
  public void testReadWrite() throws Exception {
    // create a table with the current test method's name
    TableName tn = TableName.valueOf(testName.getMethodName());
    HBaseCommitterFactory f = new HBaseCommitterFactory(util.getConfiguration(), tn);

    Committer c2 = f.readCommitter("jmhsieh");
    assertNull(c2);

    Committer c = new Committer("jmhsieh", "black", "trim", new HashMap<String,String>(0));
    f.writeCommitter(c);

    c2 = f.readCommitter("jmhsieh");

    assertEquals(c, c2);
  }

  @Test
  public void testScan() throws Exception {
    // create a table with the current test method's name
    TableName tn = TableName.valueOf(testName.getMethodName());
    HBaseCommitterFactory.setupTable(util.getConfiguration(), tn);
    HBaseCommitterFactory f = new HBaseCommitterFactory(util.getConfiguration(), tn);

    Committer[] cs = new Committer[]{
        new Committer("jmhsieh", "black", "trim", new HashMap<String, String>(0)),
        new Committer("stack", "none", null, new HashMap<String, String>(0)),
        new Committer("busbey", "brown", "scruffy", new HashMap<String, String>(0))
    };

    f.writeCommitters(Arrays.asList(cs));

    assertCount(f, "jmhsieh", null, 2); // jmhsieh to end
    assertCount(f, null, null, 3);  // beginning to end
    assertCount(f, "", "", 3); // beginning to end
    assertCount(f, null, "jmhsieh", 1); // end to jmhsieh
    assertCount(f, "stack", "jmhsieh", 1); // reorder jmhsieh upto but not including stack
    assertCount(f, "jmhsieh", "stack", 1); // natural order jmhsieh upto but not including stack
    assertCount(f, "jmhsieh", null, 2); // jmhsieh to end
  }

  void assertCount(HBaseCommitterFactory f, String start, String end, int count) throws IOException {
    int i=0;
    for (Committer c : f.scanner(start, end)) {
      i++;
    }
    assertEquals(i, count);
  }
}
