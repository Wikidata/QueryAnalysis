package metrics;

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

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementVisitorBase;
import org.apache.jena.sparql.syntax.ElementWalker;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


/**
 * @author Adrian-Bielefeldt
 * Counts the number of variables in the query.
 */
public class CountVariables implements Metric<Integer>
{
  @Override
  public final Integer analyzeQuery(Query query)
  {
    final Set<Node> variables = new HashSet<>();

    ElementWalker.walk(query.getQueryPattern(), new ElementVisitorBase()
    {
      public void visit(ElementPathBlock el)
      {
        Iterator<TriplePath> triples = el.patternElts();
        while (triples.hasNext()) {
          TriplePath next = triples.next();
          if (next.getSubject().isVariable()) {
            variables.add(next.getSubject());
          }
          if (next.getPredicate() != null) {
            if (next.getPredicate().isVariable()) {
              variables.add(next.getPredicate());
            }
          }
          if (next.getObject().isVariable()) {
            variables.add(next.getObject());
          }
        }
      }
    });
    return variables.size();
  }
}
