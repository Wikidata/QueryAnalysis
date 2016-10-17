package metrics;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

public class MetricUtils {
    static String removeComments(String queryText) {
        Query q = QueryFactory.create(queryText);
        return q.toString();
    }
}
