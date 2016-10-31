package testcases;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.nio.file.Files.readAllBytes;

import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import query.QueryHandler;

import static junit.framework.TestCase.assertEquals;

/**
 * @author jgonsior
 *         Loads the queries specified in the sparqlQueries-folder and checks
 *         if the analysis returns the results specified in the
 *         corresponding .json-files.
 *         <p>
 *         The whole test is then run as a parameterized JUnit test
 *         That means the output of getSparqlQueries is a Collection of multiple
 *         input and expected output tuples for the test case. In the end the
 *         test case is being run automatically with all tuples again and again.
 *         </p>
 */
@RunWith(Parameterized.class)
public class SparqlQueryMetricsTest
{
  /**
   * The expected input query for one test run.
   */
  private String query;

  /**
   * The expected output for one test run.
   */
  private JSONObject expected;

  /**
   * JUNit uses this constructor to give the test cases the input and expected
   * output.
   *
   * @param queryString    the test input
   * @param expectedValue  the expected output
   */
  public SparqlQueryMetricsTest(String queryString, JSONObject expectedValue)
  {
    this.query = queryString;
    this.expected = expectedValue;
  }

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
    final List<Object[]> sparqlQueriesAndExpectedOutput = new LinkedList<>();

    try (Stream<Path> paths = Files.walk(Paths.get("sparqlQueries"))) {
      paths.forEach(new Consumer<Path>()
      {
        @Override
        public void accept(Path filePath)
        {
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
        }
      });
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return sparqlQueriesAndExpectedOutput;
  }

  /**
   * Tests if the getStringLength()-metric returns the correct
   * length.
   */
  @Test
  public final void stringLength()
  {
    QueryHandler queryHandler = new QueryHandler(query);
    Integer expectedInteger = Integer.parseInt(expected.get(
        "StringLength").toString());
    assertEquals(expectedInteger, queryHandler.getStringLength());
  }

  /**
   * Test if the getStringLengthNoComments()-metric returns the correct
   * length.
   */
  public final void stringLengthNoComments()
  {
    QueryHandler queryHandler = new QueryHandler(query);
    Integer expectedInteger = Integer.parseInt(expected.get(
        "StringLengthNoComments").toString());
    assertEquals(expectedInteger, queryHandler.getStringLengthNoComments());
  }

  /**
   * Tests if the CountVariablesHead-metric returns the correct
   * number of variables.
   */
  @Test
  public final void countVariablesHead()
  {
    QueryHandler queryHandler = new QueryHandler(query);
    Integer expectedInteger = Integer.parseInt(expected.get(
        "CountVariablesHead").toString());
    assertEquals(expectedInteger, queryHandler.getVariableCountHead());
  }

  /**
   * Tests if the CountVariablesPattern-metric returns the correct
   * number of variables.
   */
  @Test
  public final void countVariablesPattern()
  {
    QueryHandler queryHandler = new QueryHandler(query);
    Integer expectedInteger = Integer.parseInt(expected.get(
        "CountVariablesPattern").toString());
    assertEquals(expectedInteger, queryHandler.getVariableCountPattern());
  }

  /**
   * Tests if the CountTriples-metric returns the correct
   * number of triples.
   */
  @Test
  public final void countTriplesWService()
  {
    QueryHandler queryHandler = new QueryHandler(query);
    Integer expectedInteger = Integer.parseInt(expected.get(
        "CountTriplesWithService").toString());
    assertEquals(expectedInteger, queryHandler.getTripleCountWithService());
  }
}
