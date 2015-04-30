import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.sequences.DocumentReaderAndWriter;
import edu.stanford.nlp.util.Triple;

import java.util.List;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RegsNER {

  public static String db() throws ClassNotFoundException
  {
	// load the sqlite-JDBC driver using the current class loader
	Class.forName("org.sqlite.JDBC");
	Connection connection = null;
	String myString = "This String was never altered from its default value!";
	
	try
	{
	  // create a database connection
	  connection = DriverManager.getConnection("jdbc:sqlite:/Users/gjm/Documents/_Works in Progress/Regulations/data/hhs_dos_comment_db.sqlite");
	  Statement statement = connection.createStatement();
	  statement.setQueryTimeout(30);  // set timeout to 30 sec.

	  // statement.executeUpdate("drop table if exists person");
	  ResultSet rs = statement.executeQuery("SELECT documentId, comment FROM documents WHERE documentId == 'CMS-2012-0031-74661' LIMIT 1");
	  while(rs.next())
	  {
		// read the result set
		System.out.println("documentId = " + rs.getString("documentId"));
		System.out.println("comment = " + rs.getString("comment"));
		myString = rs.getString("comment");
	  }
	//String myString = rs.getString("comment");   
	//System.out.println("return value = " + myString);		    
    }
    catch(SQLException e)
    {
      // if the error message is "out of memory", 
      // it probably means no database file is found
      System.err.println(e.getMessage());
    }
    finally
    {
      try
      {
        if(connection != null)
          connection.close();
      }
      catch(SQLException e)
      {
        // connection close failed.
        System.err.println(e);
      }
    }
	return myString;
  }

  public static void main(String[] args) throws Exception {
  
  	String myString = db();

    String serializedClassifier = "classifiers/english.all.3class.distsim.crf.ser.gz";

    if (args.length > 0) {
      serializedClassifier = args[0];
    }

    AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifier(serializedClassifier);

	//String[] example = {"Good afternoon Rajat Raina, how are you today?",
	//                    "I go to school at Stanford University, which is located in California." };
	String[] myStringArray = {myString};
	for (String str : myStringArray) {
	System.out.println(classifier.classifyToString(str));
	}
  }

}
