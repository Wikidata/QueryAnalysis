/**
 * 
 */
package anonymize;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;

import general.Main;
import openrdffork.StandardizingSPARQLParser;
import output.OutputHandler;
import output.OutputHandlerTSV;
import query.OpenRDFQueryHandler;
import query.QueryHandler;
import query.SparqlStatisticsCollector;
import query.QueryHandler.Validity;

/**
 * @author adrian
 *
 */
public class OutputHandlerAnonymizer extends OutputHandler
{
  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(OutputHandlerTSV.class);
  /**
   * The class of which a queryHandlerObject should be created.
   */
  private Class queryHandlerClass;
  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;
  /**
   * The outputStream object we are writing too.
   */
  private OutputStream outputStream;

  /**
   * @param fileToWrite The file to write the anonymized queries to.
   * @param queryHandlerClassToSet The query handler class to use for checking query validity.
   * @throws FileNotFoundException If the file exists but is a directory rather than a regular file,
   *                               does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public OutputHandlerAnonymizer(String fileToWrite, Class queryHandlerClassToSet) throws FileNotFoundException
  {
    super(fileToWrite, queryHandlerClassToSet);
  }

  /**
   * @param fileToWrite The file to write the anonymized queries to.
   * @param queryHandlerClassToSet The query handler class to use for checking query validity.
   * @throws FileNotFoundException If the file exists but is a directory rather than a regular file,
   *                               does not exist but cannot be created,
   *                               or cannot be opened for any other reason
   */
  public void initialize(String fileToWrite, Class queryHandlerClassToSet) throws FileNotFoundException
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
    this.queryHandlerClass = queryHandlerClassToSet;

    List<String> header = new ArrayList<>();
    header.add("#anonymizedQuery");
    writer.writeHeaders(header);
  }

  @Override
  public void writeLine(String queryToAnalyze, Validity validityStatus, String userAgent, long currentLine,
      String currentFile)
  {
    List<Object> line = new ArrayList<>();

    QueryHandler queryHandler = null;
    try {
      queryHandler = (QueryHandler) queryHandlerClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      logger.error("Failed to create query handler object" + e);
      return;
    }
    queryHandler.setValidityStatus(validityStatus);
    queryHandler.setQueryString(queryToAnalyze);

    if (queryHandler.getValidityStatus().getValue() > 0) {
      ASTQueryContainer qc;
      try {
        qc = SyntaxTreeBuilder.parseQuery(queryToAnalyze);
      }
      catch (TokenMgrError | ParseException e) {
        logger.error("Failed to parse the query although it was found valid - this is a serious bug.", e);
        return;
      }
      try {
        StandardizingSPARQLParser.debug(qc);
        StandardizingSPARQLParser.anonymize(qc);
      } catch (MalformedQueryException e) {
        logger.error("Failed to debug or anonymize query. " + queryToAnalyze);
      }
      String renderedQueryString;
      try {
        renderedQueryString = qc.jjtAccept(new RenderVisitor(), "").toString();
      }
      catch (VisitorException e) {
        logger.error("Failed to render the query.", e);
        return;
      }
      try {
        ParsedQuery parsedQuery = new StandardizingSPARQLParser().parseQuery(renderedQueryString, OpenRDFQueryHandler.BASE_URI);
      }
      catch (MalformedQueryException e) {
        logger.error("Anonymized query was not valid anymore. " + queryToAnalyze, e);
        return;
      }

      String encodedRenderedQueryString;
      try {
        encodedRenderedQueryString = URLEncoder.encode(renderedQueryString, "UTF-8");
      }
      catch (UnsupportedEncodingException e) {
        logger.error("Apparently this system does not support UTF-8. Please fix this before running the program again.");
        return;
      }
      line.add(encodedRenderedQueryString);
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
