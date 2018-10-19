/**
 *
 */
package output;

import anonymize.Anonymizer;
import general.Main;
import openrdffork.RenderVisitor;
import openrdffork.StandardizingPrefixDeclProcessor;
import openrdffork.StandardizingSPARQLParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.BaseDeclProcessor;
import org.openrdf.query.parser.sparql.StringEscapesProcessor;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import query.OpenRDFQueryHandler;
import query.QueryHandler;
import query.QueryHandler.Validity;
import query.factories.QueryHandlerFactory;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
   * The number of failed queries.
   */
  private int failedQueriesNumber = 0;

  /**
   * @param fileToWrite              The file to write the anonymized queries to.
   * @param queryHandlerFactoryToSet The query handler factory to supply the query handler to be used to check validity.
   * @throws IOException if the file exists but is a directory
   *                               rather than a regular file, does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public OutputHandlerAnonymizer(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws IOException
  {
    super(fileToWrite, queryHandlerFactoryToSet);
  }

  /**
   * @param fileToWrite              The file to write the anonymized queries to.
   * @param queryHandlerFactoryToSet The query handler class to use for checking query validity.
   * @throws IOException if the output file could not be to
   */
  public void initialize(String fileToWrite, QueryHandlerFactory queryHandlerFactoryToSet) throws IOException
  {
    if (!Main.gzipOutput) {
      outputFile = fileToWrite + ".tsv";
      bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, false), StandardCharsets.UTF_8));
    } else {
      try {
        outputFile = fileToWrite + ".tsv.gz";
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(outputFile, false)), StandardCharsets.UTF_8));
      } catch (IOException e) {
        logger.error("Somehow we are unable to write the output to " + outputFile, e);
      }
    }

    List<String> header = new ArrayList<>();
    header.add("#anonymizedQuery");
    header.add("#timestamp");
    header.add("#sourceCategory");
    header.add("#user_agent");

    csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.newFormat('\t')
        .withHeader(header.toArray(new String[header.size()]))
        .withRecordSeparator('\n')
        .withQuote('"'));

    this.queryHandlerFactory = queryHandlerFactoryToSet;
  }


  @Override
  public void writeLine(String queryToAnalyze, Validity validityStatus, String userAgent, String timeStamp, long currentLine, int currentDay,
                        String currentFile) throws IOException
  {
    List<Object> line = new ArrayList<>();

    QueryHandler queryHandler = queryHandlerFactory.getQueryHandler(validityStatus, currentLine, currentDay, queryToAnalyze, userAgent, currentFile, threadNumber);

    if (queryHandler.getValidityStatus().equals(QueryHandler.Validity.VALID)) {
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
      }
      catch (ClassCastException e) {
        logger.error("Unexpected class cast exception after anonymization.", e);
      }

      String encodedRenderedQueryString;
      try {
        encodedRenderedQueryString = URLEncoder.encode(renderedQueryString, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.error("Apparently this system does not support UTF-8. Please fix this before running the program again.");
        return;
      }
      line.add(encodedRenderedQueryString);
      line.add(timeStamp);
      if (queryHandler.getSourceCategory().equals(QueryHandler.SourceCategory.USER)) {
        line.add("organic");
      } else {
        line.add("robotic");
      }
      if (QueryHandler.isOrganicUserAgent(queryHandler.getUserAgent())) {
        line.add("browser");
      } else if (Anonymizer.allowedToolNames.contains(queryHandler.getToolName())) {
        line.add(queryHandler.getToolName());
      } else if (Anonymizer.allowedUserAgents.containsKey(queryHandler.getUserAgent())) {
        line.add(Anonymizer.allowedUserAgents.get(queryHandler.getUserAgent()));
      } else {
        line.add("other");
      }
      csvPrinter.printRecord(line);
    }
  }

  /**
   * Closes the writer this object was writing to.
   */
  public final void closeFiles()
  {
    try {
      csvPrinter.close();
    } catch (IOException e) {
      logger.error("Unable to close the outputStream ", e);
    }
  }

}
