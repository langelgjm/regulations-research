import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.sequences.DocumentReaderAndWriter;
import edu.stanford.nlp.util.Triple;

import java.util.List;
import java.util.ArrayList;
import java.io.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

// compile with javac -g -cp "*" RegsNER.java
// run with java -cp "*:." RegsNER

public class RegsNER {

	public static void main(String[] args) throws ClassNotFoundException {
		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");
		Connection connection = null;

		try
		{
			// create a database connection
			connection = DriverManager.getConnection("jdbc:sqlite:/Users/gjm/Documents/_Works in Progress/Regulations/data/hhs_dos_comment_db.sqlite");
			connection.setAutoCommit(false);
			
			
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.

			// create entities table if necessary
			CreateEntityTable(connection);

			// statement.executeUpdate("drop table if exists person");
			ResultSet rs = statement.executeQuery("SELECT documentId, comment FROM documents WHERE docketId == 'CMS-2012-0031'");
			
			String serializedClassifier = "classifiers/english.all.3class.distsim.crf.ser.gz";
			AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifier(serializedClassifier);

			while(rs.next()) {
				// read the result set
				List<String[]> entity_word_list = ExtractEntities(classifier, rs.getString("comment"));	
				for (String[] entity_word : entity_word_list) {
					InsertEntity(connection, rs.getString("documentId"), entity_word);
				}
				
				connection.commit();
			}
		} catch(SQLException e) {
			// if the error message is "out of memory", 
			// it probably means no database file is found
			System.err.println(e.getMessage());
		} catch(IOException e) {
			System.err.println(e.getMessage());
		} catch(Exception e) {
			System.err.println(e.getMessage());			
		} finally {
			try {
				if(connection != null) connection.close();
			} catch(SQLException e) {
				// connection close failed.
				System.err.println(e);
			}
		}
	}

	private static List<String[]> ExtractEntities(AbstractSequenceClassifier<CoreLabel> classifier, String text) throws Exception {
		//System.out.println(classifier.classifyToString(text));
		List<Triple<String, Integer, Integer>> entity_list = classifier.classifyToCharacterOffsets(text);

		ArrayList<String[]> return_list = new ArrayList();
		
		for (Triple<String, Integer, Integer> item : entity_list) {
			//System.out.println(item.first() + ": " + text.substring(item.second(), item.third()));
			String[] entity_word = { item.first(), text.substring(item.second(), item.third()) };
			return_list.add(entity_word);
		}
		
		return return_list;
		
	}
	
	private static void InsertEntity(Connection connection, String documentId, String[] entity_word) throws Exception {
		//System.out.println(entity_word[0] + ": " + entity_word[1]);		

		//Statement statement = connection.createStatement();
		//statement.setQueryTimeout(30);  // set timeout to 30 sec.

		PreparedStatement statement = null;
		String insertString = "INSERT INTO entities (documentId, entityType, entityString) VALUES (?, ?, ?)";
		statement = connection.prepareStatement(insertString);
		
		statement.setString(1, documentId);
		statement.setString(2, entity_word[0]);
		statement.setString(3, entity_word[1]);

		statement.executeUpdate();
		statement.close();
	}	

	private static void CreateEntityTable(Connection connection) throws Exception {
		Statement statement = connection.createStatement();
		statement.setQueryTimeout(30);  // set timeout to 30 sec.

		statement.executeUpdate("CREATE TABLE IF NOT EXISTS entities (id INTEGER PRIMARY KEY, documentId INTEGER, entityType TEXT, entityString TEXT, FOREIGN KEY(documentId) REFERENCES documents(documentId))");
        statement.close();
        connection.commit();
	}   
}
