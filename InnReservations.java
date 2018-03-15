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
import java.util.Date;

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
            ir.demo2();
        } catch (SQLException e) {
            System.err.println("SQLException: " + e.getMessage());
        }
    }

   private void run() throws SQLException {
      String input = "DARGON";
      Scanner scanner = new Scanner(System.in);
      System.out.println("Select an option: \n Rooms and Rates \n Reservations \n Quit");
      while(!input.equals("Quit")) {
         input = scanner.nextLine();
         switch(input) {
            case "Rooms and Rates":
                  System.out.println("Handled");
                  roomsAndRates();
               break;
            case "Reservations":
               //reservations();
               break;
            default:
               System.out.println("Input not recognized");
         }
      }
      // loop inputs for the basic 4
   }

   private void roomsAndRates() throws SQLException{
     String bigSelect = "select tmp1.Popularity, tmp1.Room, tmp2.Next_Avail, " + 
                     "tmp3.Last_CheckOut, tmp3.Last_Stay from (select round(sum(" + 
                     "CASE WHEN Date_Sub(CurDate(), Interval 180 DAY) between CheckIn and CheckOut " +
                     "Then datediff(CheckOut, Date_Sub(CurDate(), Interval 180 DAY)) " +
                     "WHEN CheckIn between Date_Sub(CurDate(), Interval 180 DAY) and CurDate() " +
                     "Then datediff(CheckOut, CheckIn) " +
                     "WHEN CurDate() between CheckIn and CheckOut " +
                     "Then datediff(CurDate(), CheckIn) " +
                     "Else 0 End)/180, 2) as Popularity, " +
                     "Room from lab6_reservations group by room) tmp1 " + 
                     "left join (select Room, Min(CheckOut) as Next_Avail from lab6_reservations " +
                     "where CheckOut > CurDate() " + 
                     "AND CurDate() between CheckIn and CheckOut " + 
                     "group by room " +
                     "UNION distinct " +
                     "select Room, CurDate() as Next_avail " +
                     "from lab6_reservations " +
                     "where CurDate() not between CheckIn and CheckOut " +
                     "AND CheckOut > CurDate() " +
                     ") tmp2 on tmp1.Room = tmp2.Room " +
                     "left join ( " +
                     "select Room, CheckOut as Last_CheckOut, datediff(CheckOut, CheckIn) as Last_Stay "+
                     "from lab6_reservations t1 where (Room, CheckOut) = ANY (" +
                     "select Room, max(CheckOut) from lab6_reservations " + 
                     "where CheckOut < CurDate() " +
                     "group by Room) " +
                     ") tmp3 on tmp2.Room = tmp3.Room;";
      String sql = "Select * from lab6_reservations";
      try (Connection conn = DriverManager.getConnection(System.getenv("IR_JDBC_URL"),
                                System.getenv("IR_JDBC_USER"),
                                System.getenv("IR_JDBC_PW"))) {
         try(Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
            while(rs.next()) {
               String room = rs.getString("Room");
               //float pop = rs.getFloat("Popularity");
               //Date nextAvail = rs.getDate("Next_Avail");
               //Date lastCheckout = rs.getDate("Last_CheckOut");
               //int lastStay = rs.getInt("Last_Stay");
               //System.out.format("%s %f %t %t %d\n", room, pop, nextAvail, lastCheckout, lastStay);
               System.out.format("%s\n", room);
               System.out.println("WHAT");
            }
         }
      }      
   }
   // Demo2 - Establish JDBC connection, execute SELECT query, read & print result
   private void demo2() throws SQLException {

        // Step 1: Establish connection to RDBMS
        try (Connection conn = DriverManager.getConnection(System.getenv("IR_JDBC_URL"),
                                System.getenv("IR_JDBC_USER"),
                                System.getenv("IR_JDBC_PW"))) {
            // Step 2: Construct SQL statement
            String sql = "SELECT * FROM lab6_reservations";

            // Step 3: (omitted in this example) Start transaction

            // Step 4: Send SQL statement to DBMS
            try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

            // Step 5: Receive results
            while (rs.next()) {
                String flavor = rs.getString("Room");
                System.out.format("%s ", flavor);
            }
            }

            // Step 6: (omitted in this example) Commit or rollback transaction
        }
        // Step 7: Close connection (handled by try-with-resources syntax)
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
