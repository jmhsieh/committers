package org.jmhsieh.committers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * This reads committers from HBase.
 */
public class HBaseCommitterFactory {

  final Connection c;
  final Table t;
  final TableName name;
  public final static TableName DEFAULT_NAME = TableName.valueOf("committers");
  public final static byte[] DATA_CF = Bytes.toBytes("d");
  public final static byte[] JIRA_CF = Bytes.toBytes("j");
  public final static byte[] HAIR_KEY = Bytes.toBytes("hair");
  public final static byte[] BEARD_KEY = Bytes.toBytes("beard");

  public HBaseCommitterFactory(Configuration conf, TableName tn) throws IOException {
    name = tn;
    c = ConnectionFactory.createConnection(conf);
    setupTable(conf, tn);
    t = c.getTable(name);
  }

  public HBaseCommitterFactory(Configuration conf) throws IOException {
    this(conf, DEFAULT_NAME);
  }

  /**
   * Before we can get a table, we need to make sure the table has been setup
   *
   * @param conf the cluster hbase configuration
   */
  public static void setupTable(Configuration conf, TableName name) throws IOException {
        Connection c = ConnectionFactory.createConnection(conf);
    Admin a = c.getAdmin();
    if (!a.tableExists(name)) {
      HTableDescriptor htd = new HTableDescriptor(name);
      htd.addFamily(new HColumnDescriptor(DATA_CF));
      htd.addFamily(new HColumnDescriptor(JIRA_CF));
      a.createTable(htd);
    }
  }

  // Write operations

  public void writeCommitter(Committer c) throws IOException {
    if (c == null) {
      return;
    }
    t.put(fromCommitter(c));
  }

  public void writeCommitters(Collection<Committer> cs) throws IOException {
    List<Put> l = new ArrayList(cs.size());
    for (Committer c : cs) {
      l.add(fromCommitter(c));
    }
    t.put(l);
  }

  Put fromCommitter(Committer c) {
    assert c != null;

    Put p = new Put(Bytes.toBytes(c.getName()));
    if (c.getBeard() != null)
      p.addColumn(DATA_CF, BEARD_KEY, Bytes.toBytes(c.getBeard()));
    if (c.getHair() != null)
      p.addColumn(DATA_CF, HAIR_KEY, Bytes.toBytes(c.getHair()));

    for (Map.Entry<String, String> jira : c.getJiras().entrySet()) {
      byte[] jKey = Bytes.toBytes(jira.getKey());
      byte[] jDesc = Bytes.toBytes(jira.getValue());
      p.addColumn(JIRA_CF, jKey, jDesc);
    }
    return p;
  }

  // read operations

  public Committer readCommitter(String name) throws IOException {
    Get g = new Get(Bytes.toBytes(name));
    Result r = t.get(g);
    return fromResult(r);
  }

  Committer fromResult(Result r) {
    assert r != null;
    if (r == null || r.getRow() == null) {
       return null;
    }
    String name = Bytes.toString(r.getRow());
    String hair =  Bytes.toString(r.getValue(DATA_CF, HAIR_KEY));
    String beard = Bytes.toString(r.getValue(DATA_CF, BEARD_KEY));

    Map<String, String> jiras = new TreeMap<String, String>();
    Map<byte[], byte[]> rMap  = r.getFamilyMap(JIRA_CF);
    if (rMap != null) {
      for (Map.Entry<byte[], byte[]> e : rMap.entrySet()) {
        String jira = Bytes.toString(e.getKey());
        String jiraDesc = Bytes.toString(e.getValue());
        jiras.put(jira, jiraDesc);
      }
    }
    return new Committer(name, hair, beard, jiras);
  }


  public Iterable<Committer> scanner(String startName, String endName) throws IOException {

    if (startName != null && endName != null && startName.compareTo(endName) > 0) {
      // swap if not sorted properly.
      String tmp = endName;
      endName = startName;
      startName = tmp;
    }
    Scan s = new Scan(
        (startName == null)? HConstants.EMPTY_START_ROW : Bytes.toBytes(startName),
        (endName == null) ? HConstants.EMPTY_END_ROW: Bytes.toBytes(endName));

    ResultScanner rs = t.getScanner(s);
    final Iterator<Result> i = rs.iterator();
    return new Iterable<Committer>() {

      @Override
      public Iterator<Committer> iterator() {
        return new Iterator<Committer>() {

          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public Committer next() {
            Result r = i.next();
            if (r == null) {
              return null;
            }
            return fromResult(r);
          }
        };
      }
    };
  }

}
