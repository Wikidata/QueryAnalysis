/**
 *
 */
package output;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import general.Main;
import openrdffork.RenderVisitor;
import openrdffork.StandardizingPrefixDeclProcessor;
import openrdffork.StandardizingSPARQLParser;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.BaseDeclProcessor;
import org.openrdf.query.parser.sparql.StringEscapesProcessor;
import org.openrdf.query.parser.sparql.ast.*;
import query.OpenRDFQueryHandler;
import query.QueryHandler;
import query.QueryHandler.Validity;
import query.factories.QueryHandlerFactory;

import java.io.*;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * @author adrian
 */
public class OutputHandlerAnonymizer extends OutputHandler
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(OutputHandlerAnonymizer.class);
  /**
   * The class of which a queryHandlerObject should be created.
   */
  private QueryHandlerFactory queryHandlerFactory;
  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;
  /**
   * The outputStream object we are writing too.
   */
  private OutputStream outputStream;
  /**
   * The number of failed queries.
   */
  private int failedQueriesNumber = 0;

  /**
   * @param fileToWrite              The file to write the anonymized queries to.
   * @param queryHandlerFactoryToSet The query handler factory to supply the query handler to be used to check validity.
   * @throws FileNotFoundException If the file exists but is a directory rather than a regular file,
   *                               does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public OutputHandlerAnonymizer(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws FileNotFoundException
  {
    super(fileToWrite, queryHandlerFactoryToSet);
  }

  /**
   * @param fileToWrite              The file to write the anonymized queries to.
   * @param queryHandlerFactoryToSet The query handler class to use for checking query validity.
   * @throws FileNotFoundException If the file exists but is a directory rather than a regular file,
   *                               does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public void initialize(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws FileNotFoundException
  {
    if (!Main.gzipOutput) {
      outputStream = new FileOutputStream(fileToWrite + ".tsv");
    } else {
      try {
        outputStream = new GZIPOutputStream(new FileOutputStream(new File(fileToWrite + ".tsv.gz")));
      } catch (IOException e) {
        logger.error("Somehow we are unable to write the output to " + fileToWrite + ".tsv.gz", e);
      }
    }
    writer = new TsvWriter(outputStream, new TsvWriterSettings());
    this.queryHandlerFactory = queryHandlerFactoryToSet;

    List<String> header = new ArrayList<>();
    header.add("#anonymizedQuery");
    header.add("#timestamp");
    writer.writeHeaders(header);
  }


  @Override
  public void writeLine(String queryToAnalyze, Validity validityStatus, String userAgent, String timeStamp, long currentLine, int currentDay,
                        String currentFile)
  {
    List<Object> line = new ArrayList<>();

    QueryHandler queryHandler = queryHandlerFactory.getQueryHandler(validityStatus, currentLine, currentDay, queryToAnalyze);

    if (queryHandler.getValidityStatus().getValue() > 0) {
      ASTQueryContainer qc;
      try {
        qc = SyntaxTreeBuilder.parseQuery(queryToAnalyze);
      } catch (TokenMgrError | ParseException e) {
        logger.error("Failed to parse the query although it was found valid - this is a serious bug.", e);
        return;
      }
      try {
        StandardizingSPARQLParser.debug(qc);
        StringEscapesProcessor.process(qc);
        BaseDeclProcessor.process(qc, OpenRDFQueryHandler.BASE_URI);
        StandardizingPrefixDeclProcessor.process(qc);
        StandardizingSPARQLParser.anonymize(qc);
      } catch (MalformedQueryException e) {
        logger.error("Failed to debug or anonymize query. " + queryToAnalyze);
      }
      String renderedQueryString;
      try {
        renderedQueryString = qc.jjtAccept(new RenderVisitor(), "").toString();
      } catch (VisitorException e) {
        logger.error("Failed to render the query.", e);
        return;
      }
      try {
        new StandardizingSPARQLParser().parseQuery(renderedQueryString, OpenRDFQueryHandler.BASE_URI);
      } catch (MalformedQueryException e) {
        String queryName = this.threadNumber + "_" + this.failedQueriesNumber + ".query";
        logger.error("Anonymized query was not valid anymore. " + queryName, e);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFile.substring(0, this.outputFile.lastIndexOf("/")) + "failedQueriesFolder/" + queryName))) {
          bw.write(queryToAnalyze);
          this.failedQueriesNumber++;
        } catch (IOException i) {
          logger.error("Could not write the failed query to failed queries folder.", i);
        }
        return;
      } catch (ClassCastException e) {
        logger.error("Unexpected class cast exception after anonymization.", e);
      }

      String encodedRenderedQueryString;
      try {
        encodedRenderedQueryString = URLEncoder.encode(renderedQueryString, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.error("Apparently this system does not support UTF-8. Please fix this before running the program again.");
        return;
      }
      line.add("?query=" + encodedRenderedQueryString);
      line.add(timeStamp);
      writer.writeRow(line);
    }
  }

  @Override
  public final void closeFiles()
  {
    writer.close();
    try {
      outputStream.close();
    } catch (IOException e) {
      logger.error("Unable to close the outputStream ", e);
    }
  }

}
