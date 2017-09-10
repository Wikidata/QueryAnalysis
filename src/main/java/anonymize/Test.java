package anonymize;

import static java.nio.file.Files.readAllBytes;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import general.Main;
import openrdffork.StandardizingSPARQLParser;
import query.OpenRDFQueryHandler;

public class Test
{

  public static void main(String[] args)
  {
    Main.loadStandardPrefixes();

    int worked = 0;
    int failed = 0;
    int failedToParse = 0;

    try (DirectoryStream<Path> directoryStream =
        Files.newDirectoryStream(Paths.get("/home/adrian/workspace/java/months/inputData/processedLogData/exampleQueries/"))) {
      for (Path filePath : directoryStream) {
        if (Files.isRegularFile(filePath)) {
          /*if (!filePath.endsWith("Battles_per_year_per_continent_and_country_last_80_years_(animated).exampleQuery")) {
            continue;
          }*/

          String queryString = new String(readAllBytes(filePath));

          try {
            ParsedQuery parsedQuery = new StandardizingSPARQLParser().parseQuery(queryString, OpenRDFQueryHandler.BASE_URI);
          } catch (MalformedQueryException e) {
            failedToParse++;
            continue;
          }

          ASTQueryContainer qc;
          try {
            qc = SyntaxTreeBuilder.parseQuery(queryString);
          }
          catch (TokenMgrError | ParseException e) {
            //e.printStackTrace();
            continue;
          }
          try {
            StandardizingSPARQLParser.debug(qc);
          } catch (MalformedQueryException e) {
            System.out.println("Failed to debug or anonymize query. " + queryString);
          }
          String renderedQueryString;
          try {
            renderedQueryString = qc.jjtAccept(new RenderVisitor(), "").toString();
            //System.out.println(renderedQueryString);
          }
          catch (VisitorException e) {
            //e.printStackTrace();
            continue;
          }
          try {
            ParsedQuery parsedQuery = new StandardizingSPARQLParser().parseQuery(renderedQueryString, OpenRDFQueryHandler.BASE_URI);
            worked++;
          }
          catch (MalformedQueryException | ClassCastException e) {
            failed++;
            System.out.println("-----------------------------------");
            System.out.println(filePath);
            System.out.println(queryString);
            continue;
          }

        }
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("Worked: " + worked + " Failed: " + failed + " Failed to Parse: " + failedToParse);

  }

}