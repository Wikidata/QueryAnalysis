package metrics;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.path.P_Link;
import org.apache.jena.sparql.path.Path;
import org.apache.jena.sparql.path.PathVisitorBase;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;


/**
 * @todo: use jena to count variables
 */
public class CountVariables implements Metric {
    @Override
    public Object analyseQuery(String query_string) {
		// Count the number of variables in the query
		Query query;
		try {
			query = QueryFactory.create(query_string);
		} catch (Exception e) {
			throw e;
		}
		
		Set<Node> variables  = new HashSet<Node>();
		
		ElementWalker.walk(query.getQueryPattern(), 
				new ElementVisitorBase() {
					public void visit(ElementPathBlock el) {
						Iterator<TriplePath> triples = el.patternElts();
						while (triples.hasNext()) {
							TriplePath next = triples.next();
							if (next.getSubject().isVariable()) variables.add(next.getSubject());
							if (next.getPredicate() != null) {
								if (next.getPredicate().isVariable()) variables.add(next.getPredicate());
							};
							if (next.getObject().isVariable()) variables.add(next.getObject());
						}
					}
		});
        return variables.size();
    }
}
