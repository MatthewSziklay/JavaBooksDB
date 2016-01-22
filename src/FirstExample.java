//STEP 1. Import required packages
import java.sql.*;
//Path name: /c/home/Matt/workspace/JavaBooksDB
public class FirstExample {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/BOOKS";

   //  Database credentials
   static final String USER = "username";
   static final String PASS = "password";
   
   static int authorID = 0;
   static int publisherID = 0;
   public static void main(String[] args) {
   Connection conn = null;
   Statement stmt = null;
   try{
      //STEP 2: Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      //STEP 3: Open a connection
      System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);

      //STEP 4: Execute a query
      System.out.println("Creating statement...");
      stmt = conn.createStatement();
      String sql;
      sql = "CREATE TABLE authors("
      		+ "authorID INTEGER NOT NULL, "
      		+ "firstName CHAR(20) NOT NULL, "
      		+ "lastName CHAR(20) NOT NULL,"
      		+ "PRIMARY KEY (authorID))"; 
      stmt.execute(sql);
      System.out.println("Created authors table.");
      sql = "CREATE TABLE authorISBN(" +
              "authorID INTEGER NOT NULL, " +
              "isbn CHAR(10) NOT NULL)"; 
      stmt.execute(sql); 
      System.out.println("Created authorISBN table.");
      sql = "CREATE TABLE titles(" +
              "isbn CHAR(10) NOT NULL, " +
              "title VARCHAR(500) NOT NULL, " + //VARCHAR2 resulted in a syntax error.
              "editionNumber INTEGER NOT NULL," +
              "copyright CHAR(4) NOT NULL," +
              "publisherID INTEGER NOT NULL," +
            //Not sure how to do NUMBER(8,-2), this also results in an error.
            //DECIMAL(8,2) accomplishes the desired effect.
              "price DECIMAL(8,2) NOT NULL," +
              "PRIMARY KEY (isbn))"; 
      stmt.execute(sql);
      System.out.println("Created titles table.");
      sql = "CREATE TABLE publishers("
      		+ "publisherID INTEGER NOT NULL, "
      		+ "publisherName CHAR(100) NOT NULL)";
      stmt.execute(sql); 
      System.out.println("Created publishers table.");
      //Mass insert of authors. prepInsertAuthor prepares a query that will insert a new
      //author, autoincrementing the id number.
      stmt.execute(prepInsertAuthor("Matthew","Sziklay"));
      stmt.execute(prepInsertAuthor("Dr.","Seuss"));
      stmt.execute(prepInsertAuthor("Shel","Silverstein"));
      stmt.execute(prepInsertAuthor("Charles","Schultz"));
      stmt.execute(prepInsertAuthor("Edward","Abbey"));
      stmt.execute(prepInsertAuthor("William","Shakespeare"));
      stmt.execute(prepInsertAuthor("Lewis","Carroll"));
      stmt.execute(prepInsertAuthor("Abraham","Lincoln"));
      stmt.execute(prepInsertAuthor("Mark","Twain"));
      stmt.execute(prepInsertAuthor("Stephen","King"));
      stmt.execute(prepInsertAuthor("Kurt","Vonnegut"));
      stmt.execute(prepInsertAuthor("Ernest","Hemmingway"));
      stmt.execute(prepInsertAuthor("Edgar Allan","Poe"));
      stmt.execute(prepInsertAuthor("James","Joyce"));
      stmt.execute(prepInsertAuthor("J.K","Rowling"));
 
      //Mass insert of publishers. Authors and publishers done before authorISBNs and
      //titles for dependency reasons.
      stmt.execute(prepInsertPublisher("Penguin"));
      stmt.execute(prepInsertPublisher("Very Good Publishers"));
      stmt.execute(prepInsertPublisher("Orange"));
      stmt.execute(prepInsertPublisher("New York Publishing Co."));
      stmt.execute(prepInsertPublisher("Another Publisher"));
      stmt.execute(prepInsertPublisher("New Publisher"));
      stmt.execute(prepInsertPublisher("Old Publisher"));
      stmt.execute(prepInsertPublisher("One Publisher"));
      stmt.execute(prepInsertPublisher("Two Publisher"));
      stmt.execute(prepInsertPublisher("Red Publisher"));
      stmt.execute(prepInsertPublisher("Blue Publisher"));
      stmt.execute(prepInsertPublisher("Sample 1"));
      stmt.execute(prepInsertPublisher("Sample 2"));
      stmt.execute(prepInsertPublisher("Sample 3"));
      stmt.execute(prepInsertPublisher("Sample 4"));
      
      //Mass insert of authorISBNs. Checks id before insertion to make sure it matches
      //an existing authorID.
      prepInsertAuthorISBN(1,"123456789", stmt);
      prepInsertAuthorISBN(2,"111111111", stmt);
      prepInsertAuthorISBN(3,"1111111111", stmt);
      prepInsertAuthorISBN(4,"2222222222", stmt);
      prepInsertAuthorISBN(5,"3333333333", stmt);
      prepInsertAuthorISBN(6,"4444444444", stmt);
      prepInsertAuthorISBN(7,"5555555555", stmt);
      prepInsertAuthorISBN(8,"6666666666", stmt);
      prepInsertAuthorISBN(10,"7777777777", stmt);
      prepInsertAuthorISBN(14,"8888888888", stmt);
      prepInsertAuthorISBN(61,"9999999999", stmt);
      prepInsertAuthorISBN(19,"1010101010", stmt);
      prepInsertAuthorISBN(12,"1212121212", stmt);
      prepInsertAuthorISBN(13,"1313131313", stmt);
      prepInsertAuthorISBN(17,"1414141414", stmt);
      
      //Mass insertion of titles.
      prepInsertTitle("1111111111","A title",1,"1992",5,297.33f, stmt); 
      prepInsertTitle("1414141414","A second title",4,"2010",1,300f, stmt); 
      prepInsertTitle("123456789","A third title",7,"1963",2,84f, stmt); 
      prepInsertTitle("2222222222","To Kill A Mockingbird",3,"1987",15,100f, stmt); 
      prepInsertTitle("3333333333","Ready Player One",2,"1937",3,34.1f, stmt); 
      prepInsertTitle("1010101010","Harry Potter 1",1,"2000",7,101.01f, stmt); 
      prepInsertTitle("1212121212","Monkeywrench Gang",4,"1999",1,74.2f, stmt); 
      prepInsertTitle("7777777777","The Raven",1,"2001",4,13.50f, stmt); 
      prepInsertTitle("2222222223","Alice in Wonderland",1,"2001",1,12.99f, stmt); 
      prepInsertTitle("9999999999","Example",2,"2009",5,4.99f, stmt); 
      prepInsertTitle("131313131313","Sample title",3,"2005",10,9.99f, stmt); //ISBN too long;
      prepInsertTitle("4444444444","Cat in a Hat",8,"2003",15,3.50f, stmt); 
      prepInsertTitle("5555555555","Oh the Places You'll Go",6,"2008",25,297.33f, stmt); 
      prepInsertTitle("5555555556","Armada",10,"2015",11,297.33f, stmt); 
      prepInsertTitle("6666666668","1984",1,"2013",8,297.33f, stmt); 
  
      //Select all authors + alphabetize
      prepSelectAuthors(stmt);
      
      //Select all publishers
      prepSelectPublishers(stmt);
      
      //Select publisher & list all their books. Alphabetize by title.
      prepSelectSpecificPublisher(1,stmt);
      
      //Add new author. Just call the same method we've been using.
      stmt.execute(prepInsertAuthor("Han","Solo"));
      
      //Add new title for an author.
      prepInsertTitle("0987654321","A Smuggler's Luck",16,"2055",1,999.99f, stmt);
      
      //Add new publisher.
      stmt.execute(prepInsertPublisher("Falcon Inc."));
      
      //Update an existing author.
      stmt.execute(prepUpdateAuthor(8, "Ernest", "Cline"));
      
      //Update an existing publisher.
      stmt.execute(prepUpdatePublisher(16, "A New Publisher"));
      
    //Select all authors + alphabetize
      prepSelectAuthors(stmt);
      
    //Select all publishers
      prepSelectPublishers(stmt);
      
      //These lines are simply for ease of testing the database. If they're still here,
      //I simply forgot to remove them.
      
      
      sql = "DROP TABLE publishers";
      stmt.execute(sql);
      sql = "DROP TABLE titles";
      stmt.execute(sql); 
      sql = "DROP TABLE authorISBN";
      stmt.execute(sql);
      sql = "DROP TABLE authors";
      stmt.execute(sql);
      System.out.println("DROP queries");
      //STEP 5: Extract data from result set
     
      //STEP 6: Clean-up environment
      stmt.close();
      conn.close();
   }catch(SQLException se){
      //Handle errors for JDBC
      se.printStackTrace();
   }catch(Exception e){
      //Handle errors for Class.forName
      e.printStackTrace();
   }finally{
      //finally block used to close resources
      try{
         if(stmt!=null)
            stmt.close();
      }catch(SQLException se2){
      }// nothing we can do
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }//end finally try
   }//end try
   System.out.println("Goodbye!");
}//end main
   
   //Straightforward; insert into author a generated id and supplied first/last names.
   //Initializing the table calls this 15 times. If one wished to add another author,
   //they would simply call this again with a new pair of names.
   static String prepInsertAuthor(String fn, String ln){
	   System.out.println("INSERT INTO authors " +
			   "VALUES(" + ++authorID + ", \"" + fn + "\", \"" + ln + "\")");
	   return "INSERT INTO authors " +
			   "VALUES(" + authorID + ", \"" + fn + "\", \"" + ln + "\")"; 
   }
   //Get result set containing all entries with matching authorIDs.
   //If result set is not empty, insert the new authorISBN.
   static void prepInsertAuthorISBN(int id, String isbn, Statement stmt){
	  try {
		ResultSet rs = stmt.executeQuery("SELECT * "
		   		+ "FROM authors "
		   		+ "WHERE authorID = " + id);
		if(rs.first()){
			System.out.println("INSERT INTO authorISBN "
					+ "VALUES(" + id + ",\"" + isbn +"\")");
			stmt.execute("INSERT INTO authorISBN "
					+ "VALUES(" + id + ",\"" + isbn +"\")");
		}
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   
   //Again, straightforward; insert publisher with autogenerated publisherID and supplied name.
   static String prepInsertPublisher(String pn){
	   System.out.println("INSERT INTO publishers "
	   		+ "VALUES(" + ++publisherID + ", \"" + pn + "\")");
	   return "INSERT INTO publishers "
	   		+ "VALUES(" + publisherID + ", \"" + pn + "\")" ;
   }
   
   
   //Like authorISBN, but check that publisherID exists in publishers.
   //Alphabetizes the list by lastName, then by firstName. Then prints the list in order.
   //Populating the list simply calls this 15 times. One more can be done to add another.
   static void prepInsertTitle(String isbn, String title, int editionNumber, String copyright,
		   int publisherID, float price, Statement stmt){
	   ResultSet rs;
	try {
		rs = stmt.executeQuery("SELECT * FROM publishers WHERE publisherID = " 
			   + publisherID);
		if(rs.first()){
			System.out.println("INSERT INTO titles "
			   		+ "VALUES(\"" + isbn +"\", \"" + title + "\", " + editionNumber + ", \"" + 
			   		copyright + "\", " + publisherID + ", " + price + ")");
		   stmt.execute("INSERT INTO titles "
			   		+ "VALUES(\"" + isbn +"\", \"" + title + "\", " + editionNumber + ", \"" + 
			   		copyright + "\", " + publisherID + ", " + price + ")");
	   }
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
   
   static void prepSelectAuthors(Statement stmt){
	   System.out.println("Printing all authors.");
	   try {
		ResultSet rs = stmt.executeQuery("SELECT * FROM authors ORDER BY lastName, firstName ASC");
		while(rs.next()){
			System.out.println(rs.getString("lastName") + ", " + rs.getString("firstName")
					+ ". ID: " + rs.getInt("authorID"));
		}
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
   }
   
   static void prepSelectPublishers(Statement stmt){
	   System.out.println("Selecting all publishers and printing each: ");
	    try {
			ResultSet rs = stmt.executeQuery("SELECT * FROM publishers");
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			while (rs.next()) {
			    for (int i = 1; i <= columnsNumber; i++) {
			        if (i > 1) System.out.print(",  ");
			        String columnValue = rs.getString(i);
			        System.out.print(columnValue + " " + rsmd.getColumnName(i));
			    }
			    System.out.println("");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   }
   
   //Selects a pubisher and publishes all books published by them.
   //Ordered alphabetically by title.
   static void prepSelectSpecificPublisher(int target, Statement stmt){
	   System.out.println("Printing all titles published by publisher id " + target);
	   try {
		ResultSet rs = stmt.executeQuery("SELECT * FROM titles WHERE publisherID = " + target
				+ " ORDER BY title ASC");
		while(rs.next()){
			System.out.println(rs.getString("title") + " : ISBN - " + rs.getString("isbn")
					+ " Copyright " + rs.getInt("copyright"));
		}
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	   
   }
   
   //Updates author info. id used for reference, newFN/LN used to update.
   static String prepUpdateAuthor(int id, String newFN, String newLN){
	   System.out.println("UPDATE authors "
		   		+ "SET firstName = \"" + newFN + "\", lastName = \"" + newLN
   				+ "\" WHERE authorID = " + id);
	   return "UPDATE authors "
	   		+ "SET firstName = \"" + newFN + "\", lastName = \"" + newLN
	   				+ "\" WHERE authorID = " + id;
   }
   
   //Updates publisher info. pID used for reference, newPN used to update.
   static String prepUpdatePublisher(int pID, String newPN){
	   System.out.println("UPDATE publishers "
		   		+ "SET publisherName = \"" + newPN +
		   		"\" WHERE publisherID = " + pID);
	   return "UPDATE publishers "
	   		+ "SET publisherName = \"" + newPN +
	   		"\" WHERE publisherID = " + pID;
   }
   
}//end FirstExample