package analyzer;

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

import metrics.Metric;

import java.util.LinkedList;
import java.util.List;


/**
 * @author jgonsior
 * @todo: better naming!
 */
public class SparqlQueryAnalyzer
{
  /**
   * List with all metrics being applied to supplied queries.
   */
  private List<Metric> queryAnalysers = new LinkedList<>();

  /**
   * @param metric class name of the metric to be added
   * @todo: replace choosing of metrics via class name
   */
  public final void addMetric(String metric)
  {
    try {
      Metric metricInstance = (Metric) Class.forName("metrics." + metric)
          .newInstance();
      queryAnalysers.add(metricInstance);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      System.out.println("You want to add the metric '" + metric +
          "' but it looks like we do not have such a " +
          "metric defined. Have you checked that you" +
          "have spelled it correctly?");
      e.printStackTrace();
    }
  }

  /**
   * @param query query to which the metrics set by addMetric()
   *              should be applied
   * @return returns the results of the metric application
   */
  public final Object analyse(String query)
  {
    Object output = null;
    for (Metric queryAnalyser : queryAnalysers) {
      output = queryAnalyser.analyzeQuery(query);
    }
    return output;
  }
}
