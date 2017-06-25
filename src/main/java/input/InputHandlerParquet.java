package input;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import output.OutputHandler;
import query.QueryHandler;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author adrian
 */
public class InputHandlerParquet extends InputHandler implements Serializable
{
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  /**
   * Define a static logger variable so that it references the
   * Logger instance named "InputHandler".
   */
  private static Logger logger = Logger.getLogger(InputHandlerParquet.class);
  /**
   * A map to save the index of a field name.
   */
  private final Map<String, Integer> columnNames = new HashMap<>();
  /**
   * A SparkSession for reading .parquet files and executing transformations on them.
   */
  private SparkSession spark;
  /**
   * A dataset containing the queries and metadata to be processed.
   */
  private Dataset<Row> inputDF;

  /**
   * @param fileToRead The file the parse()-method should read from.
   * @throws FileNotFoundException If the file does not exist,
   *                               is a directory rather than a regular file,
   *                               or for some other reason cannot be opened for reading.
   */
  public void setInputFile(String fileToRead) throws FileNotFoundException
  {
    this.inputFile = fileToRead;

    this.spark = SparkSession.builder().appName("QueryAnalysis").getOrCreate();
    this.inputDF = spark.read().parquet(fileToRead);

    int i = 0;
    for (String name : inputDF.schema().fieldNames()) {
      this.columnNames.put(name, i);
      i++;
    }
  }

  @Override
  public final void parseTo(final OutputHandler outputHandler)
  {
    Iterator<Row> iterator = inputDF.toLocalIterator();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Object[] convertedRow = new Object[7];
      Tuple2<String, QueryHandler.Validity> queryTuple = decode(row.get(columnNames.get("uri_query")).toString(), inputFile, -1);
      convertedRow[0] = row.get(columnNames.get("uri_query"));
      convertedRow[1] = row.get(columnNames.get("uri_path"));
      convertedRow[2] = row.get(columnNames.get("user_agent"));
      convertedRow[3] = row.get(columnNames.get("ts"));
      convertedRow[4] = row.get(columnNames.get("agent_type"));
      convertedRow[5] = row.get(columnNames.get("hour"));
      outputHandler.writeLine(queryTuple._1, queryTuple._2, convertedRow[2].toString(), -1, inputFile);
    }
    outputHandler.closeFiles();
  }
}
