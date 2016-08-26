import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.file.Files.readAllBytes;

public class Main {

    private static boolean isCorrect(String queryText) {
        QueryFactory queryFactor = new QueryFactory();
        try {
            QueryFactory.create(queryText);
        } catch (QueryParseException exception) {
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
                token -> !(token.startsWith("?") || token.startsWith("$"))
        );

        //filter out stupid duplicates
        tokens.removeIf(token -> token.endsWith(")"));

        //remove the $ or ?, because it's NOT part of the variable
        tokens = tokens.stream().map(token -> token.substring(1)).collect(Collectors.toSet());

        return tokens.size();
    }

    private static String removeComments(String query) {
        StringTokenizer stringTokenizer = new StringTokenizer(query, "\n");

        String result = "";
        //iterate over all lines of query
        while (stringTokenizer.hasMoreTokens()) {
            String token = stringTokenizer.nextToken();

            //if there is comment in this line this variable is greater than 0
            int commentBeginning = token.indexOf("#");

            //trim line to only contain the part with out the comment
            token = token.substring(0, commentBeginning >= 0 ? commentBeginning : token.length());

            result += token + "\n";
        }

        return result;
    }

    public static void main(String[] args) throws IOException {
        String query = new String(readAllBytes(Paths.get("query.sparql")));
        //System.out.println("Original Query: " + query);
        System.out.println("Is the query correct? " + isCorrect(query));
        System.out.println("How many variables does the query contain? " + countVariables(removeComments(query)));
        System.out.println("Query with all comments removed: " + removeComments(query));
    }
}