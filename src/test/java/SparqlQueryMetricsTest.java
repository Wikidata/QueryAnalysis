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
import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.Math.toIntExact;
import static java.nio.file.Files.readAllBytes;
import static junit.framework.TestCase.assertEquals;

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
                        JSONParser parser = new JSONParser();
                        array[1] = parser.parse(new FileReader("sparqlQueries/" + FilenameUtils.getBaseName(filePath.toString()) + ".json"));
                        sparqlQueriesAndExpectedOutput.add(array);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ParseException e) {
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
    public JSONObject expected;

    @Test
    public void countVariables() {
        SparqlQueryAnalyzer sparqlQueryAnalyzer = new SparqlQueryAnalyzer();
        sparqlQueryAnalyzer.addMetric("CountVariables");
        assertEquals(toIntExact((long) expected.get("CountVariables")), sparqlQueryAnalyzer.analyse(query));
    }
}