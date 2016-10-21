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

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

/**
 *
 * @author jgonsior
 *
 * Utility class to prepare queries for application of metrics.
 */
public final class MetricUtils
{
  /**
   * Setting the constructor to private to prevent initialization
   * of utility class. Also throws an AssertionError in case the
   * constructor is called.
   */
  private MetricUtils()
  {
    throw new AssertionError("Instantiating utility class...");
  }

  /**
   *
   * @param queryText query from which the comments should be removed
   * @return query without the comments
   */
  static String removeComments(String queryText)
  {
    Query q = QueryFactory.create(queryText);
    return q.toString();
  }
}
