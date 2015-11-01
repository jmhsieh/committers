package org.jmhsieh.committers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Simple web app
 */
public class CommitterApp extends AbstractHandler {
  HBaseCommitterFactory f;
  Configuration conf;
  String status;

  public CommitterApp() {
    this(HBaseConfiguration.create());
  }

  public CommitterApp(Configuration conf) {
    super();
    this.conf = conf;
    init();
  }

  void init() {
    try {
      f = new HBaseCommitterFactory(conf);
    } catch (IOException ioe) {
      ioe.printStackTrace(System.err);
    }
  }

  public void handle(String target,
                     HttpServletRequest request,
                     HttpServletResponse response,
                     int dispatch)
      throws IOException, ServletException
  {
    Request baseRequest = (request instanceof Request) ? (Request)request
        : HttpConnection.getCurrentConnection().getRequest();
    baseRequest.setHandled(true);

    response.setContentType("text/html;charset=utf-8");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println("<h1>Committers</h1>");

    response.getWriter().println("<table><tr><th>Name</th><th>Hair</th><th>Beard</th></tr>");

    if (f == null) {
      response.getWriter().println("</table>");
      response.getWriter().println("No connection to hbase.  Try again?");
      init();
      return;
    }


    Iterable<Committer> it = f.scanner(null, null);
    int i = 0;
    for (Committer c : it ) {
      response.getWriter().println("<tr>");
      response.getWriter().println("<tr><td>" + c.getName() + "</td>");
      response.getWriter().println("<td>" + c.getHair() + "</td>");
      response.getWriter().println("<td>" + c.getBeard() + "</td>");
      response.getWriter().println("</tr>");
      i++;
    }
    response.getWriter().println("</table>");
    response.getWriter().println(i + " committers");
  }

  public static void main(String[] args) throws Exception
  {
    Server server = new Server(8080);
    server.setHandler(new CommitterApp());

    server.start();
    server.join();
  }
}
