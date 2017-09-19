package anonymize;

import general.Main;
import openrdffork.RenderVisitor;
import openrdffork.StandardizingPrefixDeclProcessor;
import openrdffork.StandardizingSPARQLParser;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.BaseDeclProcessor;
import org.openrdf.query.parser.sparql.StringEscapesProcessor;
import org.openrdf.query.parser.sparql.ast.*;
import query.OpenRDFQueryHandler;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.Files.readAllBytes;

public class Test
{

  public static void main(String[] args)
  {
    Main.loadStandardPrefixes();

    Anonymizer.loadWhitelistDatatypes();

    int worked = 0;
    int failed = 0;
    int failedToParse = 0;

    try (DirectoryStream<Path> directoryStream =
             Files.newDirectoryStream(Paths.get("/home/adrian/workspace/java/months/exampleQueries/"))) {
      for (Path filePath : directoryStream) {
        if (Files.isRegularFile(filePath)) {
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
          } catch (TokenMgrError | ParseException e) {
            //e.printStackTrace();
            continue;
          }

          try {
            StandardizingSPARQLParser.debug(qc);
            StringEscapesProcessor.process(qc);
            BaseDeclProcessor.process(qc, OpenRDFQueryHandler.BASE_URI);
            StandardizingPrefixDeclProcessor.process(qc);
            StandardizingSPARQLParser.anonymize(qc);
          } catch (MalformedQueryException e) {
            System.out.println("Failed to debug or anonymize query. " + queryString);
          }
          String renderedQueryString;
          try {
            renderedQueryString = qc.jjtAccept(new RenderVisitor(), "").toString();
            //System.out.println(renderedQueryString);
          } catch (VisitorException e) {
            //e.printStackTrace();
            continue;
          }

          try {
            ParsedQuery parsedQuery = new StandardizingSPARQLParser().parseQuery(renderedQueryString, OpenRDFQueryHandler.BASE_URI);
            worked++;
          } catch (MalformedQueryException | ClassCastException e) {
            failed++;
            System.out.println("-----------------------------------");
            System.out.println(filePath);
            System.out.println(queryString);
            continue;
          }

        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println("Worked: " + worked + " Failed: " + failed + " Failed to Parse: " + failedToParse);

  }

}
