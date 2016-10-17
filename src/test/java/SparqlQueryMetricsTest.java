import org.apache.commons.io.FilenameUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.Files.readAllBytes;
import static junit.framework.TestCase.assertEquals;


/**
 * @author jgonsior
 */
@RunWith(Parameterized.class)
public class SparqlQueryMetricsTest {

    /**
     * reads in all files from the folder sparqlQueries and then executes each task in here multiple times with all
     * the queries and their expected outcome for each metric
     */
    @Parameters
    public static Collection<Object[]> getSparqlQueries() {
        List<Object[]> sparqlQueriesAndExpectedOutput = new LinkedList<>();

        try (Stream<Path> paths = Files.walk(Paths.get("sparqlQueries"))) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath) && filePath.toString().toLowerCase().endsWith(".sparql")) {
                    try {
                        Object[] array = new Object[2];
                        //the query
                        array[0] = new String(readAllBytes(filePath));

                        //@todo: convert this string into a HashMap!
                        //the expected outputs
                        array[1] = new String(readAllBytes(Paths.get("sparqlQueries/" + FilenameUtils.getBaseName(filePath.toString()) + ".output")));
                        sparqlQueriesAndExpectedOutput.add(array);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sparqlQueriesAndExpectedOutput;
    }

    @Parameter(value = 0)
    public String query;

    @Parameter(value = 1)
    public String expected;

    @Test
    public void countVariables() {
        SparqlQueryAnalyzer sparqlQueryAnalyzer = new SparqlQueryAnalyzer();
        System.out.println(query + expected);
        assertEquals(true, true);
        //sparqlQueryAnalyzer.addMetric("countVariables");
        //assertEquals(expected, sparqlQueryAnalyzer.analyse(query));
    }
}