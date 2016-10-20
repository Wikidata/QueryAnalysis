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
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.Syntax;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import static java.nio.file.Files.readAllBytes;


/**
 * @todo: - transfer countVariables and so on into seperate Metric classes
 * - ask user for metrics and queries he want's to analyse (see SparqlQueryMetricsTest) and then do the analysis
 * (and present the output to the user in a nice way)
 */
public class Main {

    private static boolean isCorrect(String queryText) {
        Query parsedQuery;
        try {
            parsedQuery = QueryFactory.create(queryText);
        } catch (QueryParseException exception) {
            return false;
        }

        Syntax test = parsedQuery.getSyntax();
        return true;
    }

    private static int countVariables(String query) {
        StringTokenizer stringTokenizer = new StringTokenizer(query);

        //get a set with all "words" from the query
        Set<String> tokens = new HashSet<>();
        while (stringTokenizer.hasMoreTokens()) {
            tokens.add(stringTokenizer.nextToken());
        }

        //filter out all elements from the set that are not starting with a ? or a $
        tokens.removeIf(
                token -> !(token.startsWith("?") || token.startsWith("$"))
        );

        //filter out stupid duplicates
        tokens.removeIf(token -> token.endsWith(")"));

        //remove the $ or ?, because it's NOT part of the variable
        tokens = tokens.stream().map(token -> token.substring(1)).collect(Collectors.toSet());

        return tokens.size();
    }

    private static String removeComments(String queryText) {
        Query q = QueryFactory.create(queryText);
        return q.toString();
    }

    public static void main(String[] args) throws IOException {
        String query = new String(readAllBytes(Paths.get("sparqlQueries/memberQuery.sparql")));
        //System.out.println("Original Query: " + query);
        System.out.println("Is the query correct? " + isCorrect(query));
        System.out.println("How many variables does the query contain? " + countVariables(removeComments(query)));
        System.out.println("Query with all comments removed: " + removeComments(query));
    }
}