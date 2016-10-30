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

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import input.InputHandler;
import output.OutputHandler;

/**
 * @author jgonsior
 */
public class Main
{

  public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException
  {
    //should be changed when it is being run on the server
    int i = 1;
    //for (int i = 1; i < 30; i++) {
    InputHandler inputHandler = new InputHandler(
        "QueryCutSept" + String.format("%02d", i) + ".tsv");
    OutputHandler outputHandler = new OutputHandler(
        "QueryProcessedSept" + String.format("%02d", i) + ".tsv");
    inputHandler.parseTo(outputHandler);
    //}
  }
}