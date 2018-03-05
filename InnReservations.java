import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import java.util.Map;
import java.util.Scanner;
import java.util.LinkedHashMap;
import java.time.LocalDate;

/*
Introductory JDBC examples (will be expanded upon during lecture) based loosely on 
the BAKERY dataset from CSC 365 labs.

-- MySQL setup:
CREATE TABLE IF NOT EXISTS lab6_rooms (
    RoomCode char(5) PRIMARY KEY,
    RoomName varchar(30) NOT NULL,
    Beds int(11) NOT NULL,
    bedType varchar(8) NOT NULL,
    maxOcc int(11) NOT NULL,
    basePrice DECIMAL(6,2) NOT NULL,
    decor varchar(20) NOT NULL,
    UNIQUE (RoomName)
);

CREATE TABLE IF NOT EXISTS lab6_reservations (
    CODE int(11) PRIMARY KEY,
    Room char(5) NOT NULL,
    CheckIn date NOT NULL,
    Checkout date NOT NULL,
    Rate DECIMAL(6,2) NOT NULL,
    LastName varchar(15) NOT NULL,
    FirstName varchar(15) NOT NULL,
    Adults int(11) NOT NULL,
    Kids int(11) NOT NULL,
    UNIQUE (Room, CheckIn),
    UNIQUE (Room, Checkout),
    FOREIGN KEY (Room) REFERENCES lab6_rooms (RoomCode)
);

INSERT INTO lab6_rooms SELECT * FROM INN.rooms;
INSERT INTO lab6_reservations SELECT CODE, Room,
DATE_ADD(CheckIn, INTERVAL 8 YEAR),
DATE_ADD(Checkout, INTERVAL 8 YEAR),
Rate, LastName, FirstName, Adults, Kids FROM INN.reservations;

-- Shell init:
export CLASSPATH=$CLASSPATH:mysql-connector-java-5.1.44-bin.jar:.
export IR_JDBC_URL=jdbc:mysql://csc365winter2018.webredirect.org/qnngo?autoReconnect=true\&useSSL=false
export IR_JDBC_USER=qnngo
export IR_JDBC_PW=...
 */

public class InnReservations {
    public static void main(String[] args) {
        try {
            InnReservations ir = new InnReservations();
            ir.demo1();
        } catch (SQLException e) {
            System.err.println("SQLException: " + e.getMessage());
        }
    }

    // Demo1 - Establish JDBC connection, execute DDL statement
    private void demo1() throws SQLException {

	// Step 0: Load MySQL JDBC Driver
	// No longer required as of JDBC 2.0  / Java 6
	try {
	    Class.forName("com.mysql.jdbc.Driver");
	    System.out.println("MySQL JDBC Driver loaded");
	} catch (ClassNotFoundException ex) {
	    System.err.println("Unable to load JDBC Driver");
	    System.exit(-1);
	}

	// Step 1: Establish connection to RDBMS
	try (Connection conn = DriverManager.getConnection(System.getenv("IR_JDBC_URL"),
							   System.getenv("IR_JDBC_USER"),
							   System.getenv("IR_JDBC_PW"))) {
	    // Step 2: Construct SQL statement
	    String sql = "ALTER TABLE hp_goods ADD COLUMN AvailUntil DATE";

	    // Step 3: (omitted in this example) Start transaction

	    try (Statement stmt = conn.createStatement()) {

		// Step 4: Send SQL statement to DBMS
		boolean exRes = stmt.execute(sql);
		
		// Step 5: Handle results
		System.out.format("Result from ALTER: %b %n", exRes);
	    }

	    // Step 6: (omitted in this example) Commit or rollback transaction
	}
	// Step 7: Close connection (handled by try-with-resources syntax)
    }
    

    // Demo2 - Establish JDBC connection, execute SELECT query, read & print result
    private void demo2() throws SQLException {

	// Step 1: Establish connection to RDBMS
	try (Connection conn = DriverManager.getConnection(System.getenv("IR_JDBC_URL"),
							   System.getenv("IR_JDBC_USER"),
							   System.getenv("IR_JDBC_PW"))) {
	    // Step 2: Construct SQL statement
	    String sql = "SELECT * FROM hp_goods";

	    // Step 3: (omitted in this example) Start transaction

	    // Step 4: Send SQL statement to DBMS
	    try (Statement stmt = conn.createStatement();
		 ResultSet rs = stmt.executeQuery(sql)) {

		// Step 5: Receive results
		while (rs.next()) {
		    String flavor = rs.getString("Flavor");
		    String food = rs.getString("Food");
		    float price = rs.getFloat("price");
		    System.out.format("%s %s: $%.2f %n", flavor, food, price);
		}
	    }

	    // Step 6: (omitted in this example) Commit or rollback transaction
	}
	// Step 7: Close connection (handled by try-with-resources syntax)
    }


    // Demo3 - Establish JDBC connection, execute DML query (UPDATE)
    // -------------------------------------------
    // Never (ever) write database code like this!
    // -------------------------------------------
    private void demo3() throws SQLException {

	// Step 1: Establish connection to RDBMS
	try (Connection conn = DriverManager.getConnection(System.getenv("IR_JDBC_URL"),
							   System.getenv("IR_JDBC_USER"),
							   System.getenv("IR_JDBC_PW"))) {
	    // Step 2: Construct SQL statement
	    Scanner scanner = new Scanner(System.in);
	    System.out.print("Enter a flavor: ");
	    String flavor = scanner.next();
	    System.out.format("Until what date will %s be available (YYYY-MM-DD)? ", flavor);
	    String availUntilDate = scanner.next();

	    // -------------------------------------------
	    // Never (ever) write database code like this!
	    // -------------------------------------------
	    String updateSql = "UPDATE hp_goods SET AvailUntil = '" + availUntilDate + "' " +
		               "WHERE Flavor = '" + flavor + "'";

	    // Step 3: (omitted in this example) Start transaction
	    
	    try (Statement stmt = conn.createStatement()) {
		
		// Step 4: Send SQL statement to DBMS
		int rowCount = stmt.executeUpdate(updateSql);
		
		// Step 5: Handle results
		System.out.format("Updated %d records for %s pastries%n", rowCount, flavor);		
	    }

	    // Step 6: (omitted in this example) Commit or rollback transaction
	    
	}
	// Step 7: Close connection (handled implcitly by try-with-resources syntax)
    }


    // Demo4 - Establish JDBC connection, execute DML query (UPDATE) using PreparedStatement / transaction    
    private void demo4() throws SQLException {

	// Step 1: Establish connection to RDBMS
	try (Connection conn = DriverManager.getConnection(System.getenv("IR_JDBC_URL"),
							   System.getenv("IR_JDBC_USER"),
							   System.getenv("IR_JDBC_PW"))) {
	    // Step 2: Construct SQL statement
	    Scanner scanner = new Scanner(System.in);
	    System.out.print("Enter a flavor: ");
	    String flavor = scanner.next();
	    System.out.format("Until what date will %s be available (YYYY-MM-DD)? ", flavor);
	    LocalDate availDt = LocalDate.parse(scanner.next());
	    
	    String updateSql = "UPDATE hp_goods SET AvailUntil = ? WHERE Flavor = ?";

	    // Step 3: Start transaction
	    conn.setAutoCommit(false);
	    
	    try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
		
		// Step 4: Send SQL statement to DBMS
		pstmt.setDate(1, java.sql.Date.valueOf(availDt));
		pstmt.setString(2, flavor);
		int rowCount = pstmt.executeUpdate();
		
		// Step 5: Handle results
		System.out.format("Updated %d records for %s pastries%n", rowCount, flavor);

		// Step 6: Commit or rollback transaction
		conn.commit();
	    } catch (SQLException e) {
		conn.rollback();
	    }

	}
	// Step 7: Close connection (handled implcitly by try-with-resources syntax)
    }


}