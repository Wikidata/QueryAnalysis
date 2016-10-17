package metrics;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;


/**
 * @todo: use jena to count variables
 */
public class CountVariables implements Metric {
    @Override
    public Object analyseQuery(String query) {
        query = MetricUtils.removeComments(query);
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
}
