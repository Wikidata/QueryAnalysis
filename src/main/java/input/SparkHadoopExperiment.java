package input;

import logging.LoggingHandler;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author adrian
 *
 */
public final class SparkHadoopExperiment
{
  /**
   * Since this is a utility class, it should not be instantiated.
   */
  private SparkHadoopExperiment()
  {
    throw new AssertionError("Instantiating utility class SparkHadoopExperiment");
  }

  /**
   * @param args Arguments to specify runtime behavior.
   */
  public static void main(String[] args)
  {
    LoggingHandler.initFileLog("logsExperimental/logging.%timestamp.log", Level.INFO);
    SparkConf conf = new SparkConf().setAppName("SPARQLQueryAnalyzer_Experimental").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
  }

}
