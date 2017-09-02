package anonymize;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.Files.readAllBytes;

import general.Main;
import logging.LoggingHandler;
import openrdffork.StandardizingSPARQLParser;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

/**
 * @author adrian
 *
 */
public class Anonymizer
{

  /**
   * Define a static logger variable.
   */
  private static final Logger logger = Logger.getLogger(Main.class);

  /**
   * @param args
   */
  public static void main(String[] args)
  {
    Main.loadStandardPrefixes();

    LoggingHandler.initFileLog("Anonymizer", "nothing");

    int worked = 0;
    int failed = 0;
    int messedUp = 0;

    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get("/home/adrian/workspace/java/months/inputData/processedLogData/exampleQueries/"))) {
      for (Path filePath : directoryStream) {
        if (Files.isRegularFile(filePath)) {
          // String queryString = new String(readAllBytes(filePath));
          String queryString = "SELECT ?item ?itemLabel ?adjacent ?adjacentL ?coords {  ?item wdt:P31/wdt:P279* wd:Q928830 ;        wdt:P81 wd:Q13224 ;        wdt:P625 ?coords .  OPTIONAL {    ?item p:P197 [ ps:P197 ?adjacent ; pq:P560 wd:Q585752 ] .    ?adjacent rdfs:label ?adjacentL filter (lang(?adjacentL) = \"en\")  }  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" . }} ORDER BY ?itemLabel";
          String renderedQueryString = "";

          try {
            ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(queryString);
            renderedQueryString = (String) qc.jjtAccept(new RenderVisitor(), "");
            System.out.println(renderedQueryString);
            System.exit(0);
          }
          catch (TokenMgrError | ParseException e) {
            continue;
          }
          catch (VisitorException e) {
            e.printStackTrace();
          }

          try {
            ParsedQuery parsedQuery = new StandardizingSPARQLParser().parseQuery(renderedQueryString, query.OpenRDFQueryHandler.BASE_URI);
            worked++;
          }
          catch (MalformedQueryException e) {
            System.out.println("--------------------------------------------------------");
            System.out.println(queryString);
            failed++;
          }
        }
      }
    }
    catch (IOException e1) {
      e1.printStackTrace();
    }

    System.out.println("Worked: " + worked + " Failed: " + failed);

    /*
    int worked = 0;
    int failed = 0;
    int failedToParse = 0;

    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get("/home/adrian/workspace/java/months/inputData/processedLogData/exampleQueries/"))) {
      for (Path filePath : directoryStream) {
        if (Files.isRegularFile(filePath)) {
          //String queryString = new String(readAllBytes(filePath));
          String queryString = "SELECT * WHERE { ?x wdt:P31 wd:Q123 }";
          String renderedQueryString = "";

          try {
            ParsedQuery parsedQuery = new StandardizingSPARQLParser().parseQuery(queryString, query.OpenRDFQueryHandler.BASE_URI);
            renderedQueryString = new SPARQLQueryRenderer().render(parsedQuery);
          }
          catch (MalformedQueryException e) {
            failedToParse += 1;
            if (!e.toString().matches(".*projection alias .* was previously used.*")) {
              System.out.println(queryString);
              String filePathString = filePath.toString();
              System.out.println(filePathString.substring(filePathString.lastIndexOf("/"), filePathString.length()));
              System.out.println(e.getMessage());
            }
            continue;
          }
          catch (Exception e) {
            // e.printStackTrace();
            continue;
          }
          try {
            ParsedQuery parsedRenderedQuery = new StandardizingSPARQLParser().parseQuery(renderedQueryString, query.OpenRDFQueryHandler.BASE_URI);
            worked += 1;
          }
          catch (MalformedQueryException e) {
            failed += 1;
          }
        }
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("Worked: " + worked + " Failed: " + failed + " Failed to parse: " + failedToParse);
    */
  }
}
