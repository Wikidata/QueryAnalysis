/*-
 * #%L
 * sparqlQueryTester
 * %%
 * Copyright (C) 2016 QueryAnalysis
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import QueryHandler.InvalidQueryException;
import QueryHandler.QueryHandler;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * @author jgonsior
 */
public class Main
{

  public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException
  {
    //read in queries from tsv
    TsvParserSettings parserSettings = new TsvParserSettings();
    parserSettings.setLineSeparatorDetectionEnabled(true);
    parserSettings.setHeaderExtractionEnabled(true);

    ObjectRowProcessor rowProcessor = new ObjectRowProcessor()
    {
      @Override
      public void rowProcessed(Object[] row, ParsingContext parsingContext)
      {
        String queryString = "";
        try {
          //parse url
          List<NameValuePair> params = URLEncodedUtils.parse(new URI(((String) row[0])), "UTF-8");

          //find out the query parameter
          for (NameValuePair param : params) {
            if (param.getName().equals("query")) {
              queryString = param.getValue();
            }
          }
        } catch (URISyntaxException e) {
          System.out.println("There was a syntax error in the following URI: " + row[0]);
        }
        QueryHandler queryHandler = new QueryHandler(queryString);

        try {
          //in case that query is valid
          System.out.println("Number of Variables: " +
              queryHandler.getVariableCount());
          System.out.println("Number of Triples: " + queryHandler.getTripleCount());
        } catch (InvalidQueryException exception) {
          //save query as invalid
          System.out.println("Invalid query: " + queryString);
        }

        //persist metrics and so on in tsv

      }

    };

    parserSettings.setRowProcessor(rowProcessor);

    TsvParser parser = new TsvParser(parserSettings);

    //should be changed when it is being run on the server
    int i = 1;
    //for (int i = 1; i < 30; i++) {
    parser.parse(new InputStreamReader(new FileInputStream("QueryCutSept" + String.format("%02d", i) + ".tsv"), "UTF-8"));
    //}
  }
}