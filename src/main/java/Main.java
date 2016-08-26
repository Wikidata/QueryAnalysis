import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.DatasetDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParserFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import static java.nio.file.Files.readAllBytes;

public class Main {

    private static boolean isCorrect(String queryText) {
        SPARQLParserFactory sparqlParserFactory = new SPARQLParserFactory();
        QueryParser parser = sparqlParserFactory.getParser();
        try {
            parser.parseQuery(queryText, "http://wikiba.se/ontology#");
        } catch (MalformedQueryException e) {
            return false;
        }
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
                p -> !(p.startsWith("?") || p.startsWith("$"))
        );


        return tokens.size();
    }

    private static String removeComments(String query) {
        return query;
    }

    public static void main(String[] args) throws IOException {
        String query = new String(readAllBytes(Paths.get("query.sparql")));
        //System.out.println("Original Query: " + query);
        System.out.println("Is the query correct? " + isCorrect(query));
        System.out.println("How many variables does the query contain? " + countVariables(query));
        //System.out.println("Query string with all comments removed: " + removeComments(query));
    }
}