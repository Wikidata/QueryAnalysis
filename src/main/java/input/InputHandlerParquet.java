package input;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import output.OutputHandler;
import query.QueryHandler;

/**
 * @author adrian
 *
 */
public class InputHandlerParquet extends InputHandler
{
  /**
   * Define a static logger variable so that it references the
   * Logger instance named "InputHandler".
   */
  private static Logger logger = Logger.getLogger(InputHandlerParquet.class);
  /**
   * A map to save the index of a field name.
   */
  private Map <String, Integer> columnNames = new HashMap<String, Integer>();
  /**
   * A SparkSession for reading .parquet files and executing transformations on them.
   */
  private SparkSession spark;
  /**
   * A dataset containing the queries and metadata to be processed.
   */
  private Dataset<Row> inputDF;
  /**
   * The name of the input file for referencing in the output file.
   */
  private String inputFile;

  /**
   *
   */
  public InputHandlerParquet()
  {
    SparkConf conf = new SparkConf().setAppName("SPARQLQueryAnalyzer_Experimental").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);
    this.spark = SparkSession.builder().appName("QueryAnalysis").getOrCreate();
  }

  @Override
  public final void parseTo(final OutputHandler outputHandler)
  {
    inputDF.foreach(new ForeachFunction<Row>()
    {
      @Override
      public void call(Row row) throws Exception
      {
        /*
        String queryString = decode(row.get(columnNames.get("uri_query")).toString(), inputFile, "unknown for parquet");
        Object[] convertedRow = new Object[6];
        convertedRow[0] = row.get(columnNames.get("uri_query"));
        convertedRow[1] = row.get(columnNames.get("uri_path"));
        convertedRow[2] = row.get(columnNames.get("user_agent"));
        convertedRow[3] = row.get(columnNames.get("ts"));
        convertedRow[4] = row.get(columnNames.get("agent_type"));
        convertedRow[5] = row.get(columnNames.get("hour"));
        System.out.println(queryString);
        System.out.println(convertedRow);
        */
      }
    });
  }

  @Override
  public final void setInputFile(String fileToRead) throws FileNotFoundException
  {
    this.inputFile = fileToRead;
    this.inputDF = spark.read().parquet(fileToRead);

    int i = 0;
    for (String name:inputDF.schema().fieldNames()) {
      this.columnNames.put(name, i);
      i++;
    }
  }

}
