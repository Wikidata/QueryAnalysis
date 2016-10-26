package analyzer;

/*-
 * #%L
 * sparqlQueryTester
 * %%
 * Copyright (C) 2016 QueryAnalysis
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import metrics.Metric;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryFactory;
import org.h2.jdbcx.JdbcConnectionPool;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * @author jgonsior
 * @todo: better naming!
 */
public class SparqlQueryAnalyzer
{
  /**
   * List with all metrics being applied to supplied queries.
   */
  private List<Metric> queryAnalysers = new LinkedList<>();

  /**
   * @param metric class name of the metric to be added
   * @todo: replace choosing of metrics via class name
   */
  public final void addMetric(String metric)
  {

    try {
      Metric metricInstance = (Metric) Class.forName("metrics." + metric)
          .newInstance();
      queryAnalysers.add(metricInstance);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("You want to add the metric '" + metric +
          "' but it looks like we do not have such a " +
          "metric defined. Have you checked that you" +
          "have spelled it correctly?");
      e.printStackTrace();
    }

  }

  /**
   * @param queryString query to which the metrics set by addMetric()
   *                    should be applied
   * @return returns the results of the metrics as a Map where the key is the
   * name of the metric and the value is the result of this metric
   */
  public final Map<String, Object> analyse(String queryString)
  {
    Query query;
    try {
      query = QueryFactory.create(queryString);
    } catch (QueryException e) {
      //this query is probably syntactically wrong?
      throw e;
    }

    Map<String, Object> output = new HashMap<>();
    for (Metric queryAnalyser : queryAnalysers) {
      output.put(queryAnalyser.getClass().getSimpleName(),
          queryAnalyser.analyzeQuery(query));
    }
    return output;
  }

  /**
   * saves the query and the associated output from the metrics into a database
   * table. To enable quicker matchings of queries which we have been processed
   * already the md5 hash is computed and saved (may be useful later)
   *
   * @param queryString
   * @param metricOutput
   * @throws NoSuchAlgorithmException
   * @throws UnsupportedEncodingException
   */
  public void saveOutputToDatabase(String queryString, Map<String, Object> metricOutput)
      throws NoSuchAlgorithmException, UnsupportedEncodingException
  {
    //using in-memory H2 database, should be changed later :)
    DataSource dataSource = JdbcConnectionPool.create("jdbc:h2:mem:database",
        "username",
        "password");

    DBI dbi = new DBI(dataSource);
    Handle handle = dbi.open();

    //create a database for the queries and their checksums
    handle.execute("CREATE TABLE queries (id INT PRIMARY KEY, query LONGVARCHAR, " +
        "checksum VARCHAR(150))");

    //create for each metric a new database table
    for (Metric metric : this.queryAnalysers) {
      handle.execute("CREATE TABLE " + metric.getClass().getSimpleName() +
          " (id INT PRIMARY KEY, queryId INT, output VARCHAR(150))");
    }

    MessageDigest messageDigest = MessageDigest.getInstance("MD5");

    //should be added automatically by a more intelligent database
    int queryId = 1;

    //insert query into queries table
    handle.execute("INSERT INTO queries (id, query, checksum) VALUES (?, ?, ?)",
        queryId, queryString, new String(messageDigest.digest(queryString.getBytes("UTF-8"))));

    //insert metricOutput into the table for this metric
    for (Map.Entry<String, Object> entry : metricOutput.entrySet()) {

      //should be added automatically by a more intelligent database
      int metricId = 42;
      handle.execute("INSERT INTO " + entry.getKey() + "(id, queryId, output)" +
          "VALUES (?,?,?)", metricId, queryId, entry.getValue());
    }

    handle.close();
  }

  /**
   * quickly dumps the content of the database
   */
  public void displayDatabase()
  {
    //using in-memory H2 database, should be changed later :)
    DataSource dataSource = JdbcConnectionPool.create("jdbc:h2:mem:database",
        "username",
        "password");

    DBI dbi = new DBI(dataSource);
    Handle handle = dbi.open();

    for (Map queryResult : handle.createQuery("SELECT * FROM queries").list()) {
      System.out.println("Query id: " + queryResult.get("id") + "  checksum: " + queryResult.get("checksum") + "\n\n");
      System.out.println(queryResult.get("query"));

      for (Metric metric : this.queryAnalysers) {
        for (Map metricResult : handle.createQuery("SELECT * FROM " + metric.getClass().getSimpleName()).list()) {
          System.out.println("Metric " + metric.getClass().getSimpleName() + "\t output: " + metricResult.get("output"));
        }
      }
    }
  }
}
