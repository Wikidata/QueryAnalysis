package output;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;

/**
 *
 * @author adrian
 *
 */
public class OutputHandler
{
  /**
   * A writer created at object creation to be used in line-by-line writing.
   */
  private TsvWriter writer;
  /**
   * Creates the file specified in the constructor and writes the header.
   * @param fileToWrite location of the file to write the received values to
   * @throws FileNotFoundException if the file exists but is a directory
   * rather than a regular file, does not exist but cannot be created,
   * or cannot be opened for any other reason
   */
  public OutputHandler(String fileToWrite) throws FileNotFoundException
  {
    FileOutputStream outputWriter = new FileOutputStream(fileToWrite);
    writer = new TsvWriter(outputWriter, new TsvWriterSettings());

  }

  /**
   * Closes the writer this object was writing to.
   */
  public final void closeFile()
  {
    writer.close();
  }

  /**
   * Takes a query and the additional information from input and writes
   * the available data to the active .tsv.
   */
  public final void writeLine()
  {

  }
}
