import metrics.Metric;

import java.util.LinkedList;
import java.util.List;

/**
 * @todo: better naming!
 */
public class SparqlQueryAnalyzer {

    private List<Metric> queryAnalysers = new LinkedList<>();

    public void addMetric(String metric) {
        try {
            Metric metricInstance = (Metric) Class.forName("metrics." + metric).newInstance();
            queryAnalysers.add(metricInstance);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("You want to add the metric '" + metric + "' but it looks like we do not have such a " +
                    "metric defined. Have you checked that you have spelled it correctly?");
            e.printStackTrace();
        }

    }

    public Object analyse(String query) {
        Object output = null;
        for (Metric queryAnalyser : queryAnalysers) {
            output = queryAnalyser.analyseQuery(query);
        }
        return output;
    }
}
