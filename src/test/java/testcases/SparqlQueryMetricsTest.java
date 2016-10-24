package testcases;

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

import analyzer.SparqlQueryAnalyzer;
import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.Math.toIntExact;
import static java.nio.file.Files.readAllBytes;
import static junit.framework.TestCase.assertEquals;

/**
 * @author jgonsior
 *         Loads the queries specified in the sparqlQueries-folder and checks
 *         if the analysis returns the results specified in the
 *         corresponding .json-files.
 *         <p>
 *         The whole test is then run as a parameterized JUnit test
 *         That means the output of getSparqlQueries is a Collection of multiple
 *         input and expected output tuples for the test case. In the end the test
 *         case is being run automatically with all tuples again and again.
 *         </p>
 */
@RunWith(Parameterized.class)
public class SparqlQueryMetricsTest
{
  /**
   * The expected input query for one test run
   */
  private String query;

  /**
   * The expected output for one test run
   */
  private JSONObject expected;

  /**
   * Reads in all files from the folder sparqlQueries and then executes each
   * task in here multiple times with all the queries and their expected outcome
   * for each metric.
   *
   * @return list of tuples with a query to be analyzed and the expected result
   */
  @Parameters
  public static Collection<Object[]> getSparqlQueries()
  {
    List<Object[]> sparqlQueriesAndExpectedOutput = new LinkedList<>();

    try (Stream<Path> paths = Files.walk(Paths.get("sparqlQueries"))) {
      paths.forEach(filePath -> {
        if (Files.isRegularFile(filePath) &&
            filePath.toString().toLowerCase().endsWith(".sparql")) {
          try {
            Object[] array = new Object[2];
            // the query
            array[0] = new String(readAllBytes(filePath));

            // @todo: convert this string into a HashMap!
            // the expected outputs
            JSONParser parser = new JSONParser();
            array[1] = parser.parse(new FileReader("sparqlQueries/" +
            FilenameUtils.getBaseName(filePath.toString()) + ".json"));
            sparqlQueriesAndExpectedOutput.add(array);
          }
          catch (IOException e) {
            e.printStackTrace();
          }
          catch (ParseException e) {
            e.printStackTrace();
          }
        }
      });
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return sparqlQueriesAndExpectedOutput;
  }

  /**
   * JUNit uses this constructor to give the test cases the input and expected
   * output
   *
   * @param query    the test input
   * @param expected the expected output
   */
  public SparqlQueryMetricsTest(String query, JSONObject expected)
  {
    this.query = query;
    this.expected = expected;
  }

  /**
   * Tests whether the CountVariables-metric returns the correct
   * number of variables.
   */
  @Test
  public final void countVariables()
  {
    SparqlQueryAnalyzer sparqlQueryAnalyzer = new SparqlQueryAnalyzer();
    sparqlQueryAnalyzer.addMetric("CountVariables");
    Map result = sparqlQueryAnalyzer.analyse(query);
    assertEquals(toIntExact((long) expected.get("CountVariables")),
        sparqlQueryAnalyzer.analyse(query).get("class metrics.CountVariables"));
  }
}
