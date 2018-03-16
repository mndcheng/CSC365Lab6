import java.time.LocalDate;
import java.util.*;
import java.sql.*;
import java.text.*;
import java.time.*;

/*
-- Shell init:
export CLASSPATH=$CLASSPATH:mysql-connector-java-5.1.44-bin.jar:.
export LAB6_JDBC_URL=jdbc:mysql://csc365winter2018.webredirect.org/qnngo?autoReconnect=true\&useSSL=false
export LAB6_JDBC_USER=qnngo
export LAB6_JDBC_PW=...

export CLASSPATH=$CLASSPATH:mysql-connector-java-5.1.45-bin.jar:.ls
export LAB6_JDBC_URL=jdbc:mysql://csc365winter2018.webredirect.org/acheng21?autoReconnect=true\&useSSL=false
export LAB6_JDBC_USER=acheng21
export LAB6_JDBC_PW=365W18_011282748
 */

public class InnReservations {

	static final int FN = 0, LN = 1, D1 = 2, D2 = 3, RMC = 4, RSC = 5;

    public static void main(String[] args) {
        try {
			Scanner sc = new Scanner(System.in);
			System.out.print("What would you like to do:\n" + 
							"Look at rooms and rates (RNR)\n" + 
							"Make a reservation (RES)\n" + 
							"Look at reservation information (RESI)\n" + 
							"View revenue (REV)\n" +
							"Quit the program (QUIT)?\n" +
							"Enter here: ");
			InnReservations innRes = new InnReservations();
			
			while (!(sc.hasNext("QUIT"))) {
				String input = sc.nextLine();
				System.out.println("");

				if (input.equals("RESI")) {
					innRes.reservationInfo();
				} else if (input.equals("REV")) {
					innRes.revenue();
				} else if (input.equals("RNR")) {
					innRes.roomsAndRates();
				} else if (input.equals("RES")) {
					innRes.reservations(); 
				}

				System.out.print("What would you like to do:\n" + 
							"Look at rooms and rates (RNR)\n" + 
							"Make a reservation (RES)\n" + 
							"Look at reservation information (RESI)\n" + 
							"View revenue (REV)\n" +
							"Quit the program (QUIT)?\n" +
							"Enter here: ");
			}
			
		} catch (SQLException e) {
			System.err.println("SQLException: " + e.getMessage());
		}
    }
	
   private void roomsAndRates() throws SQLException{
     String bigSelect = "SELECT tmp1.Popularity, tmp1.Room, tmp2.Next_Avail, " +
                     "tmp3.Last_CheckOut, tmp3.Last_Stay FROM (SELECT ROUND(SUM(" +
                     "CASE WHEN Date_Sub(CurDate(), Interval 180 DAY) BETWEEN CheckIn AND CheckOut " +
                     "THEN DATEDIFF(CheckOut, Date_Sub(CurDate(), Interval 180 DAY)) " +
                     "WHEN CheckIn BETWEEN Date_Sub(CurDate(), Interval 180 DAY) AND CurDate() " +
                     "THEN datediff(CheckOut, CheckIn) " +
                     "WHEN CurDate() BETWEEN CheckIn AND CheckOut " +
                     "THEN DATEDIFF(CurDate(), CheckIn) " +
                     "ELSE 0 END)/180, 2) AS Popularity, " +
                     "Room FROM lab6_reservations GROUP BY room) tmp1 " +
                     "LEFT JOIN (SELECT Room, MIN(CheckOut) AS Next_Avail FROM lab6_reservations " +
                     "WHERE CheckOut > CurDate() " +
                     "AND CurDate() BETWEEN CheckIn AND CheckOut " +
                     "GROUP BY room " +
                     "UNION DISTINCT " +
                     "SELECT Room, CurDate() AS Next_avail " +
                     "FROM lab6_reservations " +
                     "WHERE Room NOT IN ( " +
                     "SELECT Room FROM lab6_reservations WHERE CurDate() BETWEEN CheckIn AND CheckOut)" +
                     ") tmp2 ON tmp1.Room = tmp2.Room " +
                     "LEFT JOIN ( " +
                     "SELECT Room, CheckOut AS Last_CheckOut, DATEDIFF(CheckOut, CheckIn) AS Last_Stay "+
                     "FROM lab6_reservations t1 WHERE (Room, CheckOut) = ANY (" +
                     "SELECT Room, MAX(CheckOut) FROM lab6_reservations " +
                     "WHERE CheckOut < CurDate() " +
                     "GROUP BY Room) " +
                     ") tmp3 ON tmp2.Room = tmp3.Room;";
      DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
      try (Connection conn = DriverManager.getConnection(System.getenv("LAB6_JDBC_URL"),
                                System.getenv("LAB6_JDBC_USER"),
                                System.getenv("LAB6_JDBC_PW"))) {
         try(Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(bigSelect)) {
            System.out.format("%-10s %-10s %-15s %-15s %-15s\n", "Room", "Popularity", "Next Avail",
                              "Prev Checkout", "Prev Stay");
            while(rs.next()) {
               String room = rs.getString("Room");
               float pop = rs.getFloat("Popularity");

               java.util.Date nextAvail = rs.getDate("Next_Avail");
               String nextAvailS = df.format(nextAvail);
               java.util.Date lastCheckout = rs.getDate("Last_CheckOut");
               String lastCheckoutS = df.format(lastCheckout);

               int lastStay = rs.getInt("Last_Stay");
               System.out.format("%-10s %-10.2f %-15s %-15s %-15d\n", room,
                                 pop, nextAvailS, lastCheckoutS, lastStay);
            }
            System.out.println();
         }
      }
   }
   private void reservations() {
      Scanner scan = new Scanner(System.in);
      System.out.println("Enter first name for reservation: ");
      String firstName = scan.nextLine();

      System.out.println("Enter last name for reservation: ");
      String lastName = scan.nextLine();

      System.out.println("Enter room code or \"any\" if no preference: ");
      String roomCode = scan.nextLine();

      System.out.println("Enter preferred bedtype or \"any\" if no preference: ");
      String bedType = scan.nextLine();

      System.out.println("Enter desired check-in date: ");
      String checkIn = scan.nextLine();

      System.out.println("Enter desired check-out date: ");
      String checkOut = scan.nextLine();

      System.out.println("Enter number of children: ");
      String children = scan.nextLine();
      int numChildren = Integer.parseInt(children);

      System.out.println("Enter number of adults: ");
      String adults = scan.nextLine();
      int numAdults = Integer.parseInt(adults);

      if((numChildren + numAdults) > 4) {
         System.out.println("Too many occupants for any room\n");
      } else {
         try {
            suggestions(firstName, lastName, roomCode, bedType, checkIn, checkOut, numChildren, numAdults);
         }
         catch(SQLException e) {}
      }
   }
	
   private void suggestions(String firstName, String lastName, String roomCode, String bedType,
                           String checkIn, String checkOut,
                           int numChildren, int numAdults) throws SQLException {

      List<Reservation> exactMatches = exactSuggestions(roomCode, bedType, checkIn,
                                                checkOut, numChildren, numAdults);
      Reservation res;

      Scanner scan = new Scanner(System.in);

      String insert = "Insert Into lab6_reservations ?, ?, ?, ?, ?, ?, ?, ?";
      if(exactMatches.size() > 0) {
         int i = 1, counter = 1;
         System.out.println("Exact Matches Found: ");
         for(Reservation r : exactMatches) {
            System.out.printf("%-3d %s\n", i++, r.getRoomCode());
         }

         System.out.println("Select a number: ");
         String resp = scan.nextLine();
         int selection = Integer.parseInt(resp);
         res = exactMatches.get(selection - 1);

         float total = calcCost(res.getRate(), checkIn, checkOut);

         System.out.println("Reservation Info:");
         System.out.printf("First Name: %s\n", firstName);
         System.out.printf("Last Name: %s\n", lastName);

         printReservation(res, total);

         System.out.println("confirm or cancel");
         String confirm = scan.nextLine();

         if(confirm.equals("confirm")) {
            try (Connection conn = DriverManager.getConnection(System.getenv("LAB6_JDBC_URL"),
                                   System.getenv("LAB6_JDBC_USER"),
                                   System.getenv("LAB6_JDBC_PW"))) {
               try(PreparedStatement ps = conn.prepareStatement(insert)) {
                  ps.setInt(counter++, ); // NEEDS A UNIQUE ID
                  ps.setString(counter++, res.getRoomCode());
                  ps.setString(counter++, res.getCheckIn());
                  ps.setString(counter++, res.getCheckOut());
                  ps.setFloat(counter++, total);
                  ps.setString(counter++, lastName);
                  ps.setString(counter++, firstName);
                  ps.setInt(counter++, res.getNumAdults());
                  ps.setInt(counter++, res.getNumChildren());
               }
            }
         }
      }
      else {
         
      }
   }
   private void printReservation(Reservation res, float total) {
      System.out.printf("Room Code: %s\n", res.getRoomCode());
      System.out.printf("Room Name: %s\n", res.getRoomName());
      System.out.printf("Bed Type: %s\n", res.getBedType());
      System.out.printf("Check In Date: %s\n", res.getCheckIn());
      System.out.printf("Check Out Date: %s\n", res.getCheckOut());
      System.out.printf("Number of Children: %d\n", res.getNumChildren());
      System.out.printf("Number of Adults: %d\n", res.getNumAdults());
      System.out.printf("Total Cost: %.2f\n", total);
   }

   private float calcCost(float rate, String checkIn, String checkOut) {
    int numWeekdays = 0, numWeekends = 0;
    LocalDate cko, ckin;
    cko = LocalDate.parse(checkOut);
    ckin = LocalDate.parse(checkIn);
    while(!ckin.equals(cko)) {
      if(ckin.getDayOfWeek() == DayOfWeek.SUNDAY || ckin.getDayOfWeek() == DayOfWeek.SATURDAY)
         numWeekends++;
      else
         numWeekdays++;
      ckin = ckin.plusDays(1);
      }
      return (float)(rate*numWeekdays) + (float)(rate*1.1*numWeekends);
   }
      
	private List<Reservation> exactSuggestions(String roomCode, String bedType,
                                 String checkIn, String checkOut,
                                 int numChildren, int numAdults) throws SQLException {

      List<Reservation> ls = new ArrayList<Reservation>();
      String sqlSelect = "Select RoomCode, RoomName, BedType, BasePrice from lab6_rooms where ";
      int counter = 1;

      if(!roomCode.equals("any")) {
         sqlSelect = sqlSelect + "RoomCode = ? AND ";
      }

      if(!bedType.equals("any")) {
         sqlSelect = sqlSelect + "bedType = ? AND ";
      }

      sqlSelect = sqlSelect + "maxOcc >= ? AND " +
                        "RoomCode NOT IN (Select Room from lab6_reservations " +
                        "where CheckIn between ? " +
                        "and ? OR CheckOut " +
                        "between ? and ?);";
      try (Connection conn = DriverManager.getConnection(System.getenv("LAB6_JDBC_URL"),
                                System.getenv("LAB6_JDBC_USER"),
                                System.getenv("LAB6_JDBC_PW"))) {
         try(PreparedStatement ps = conn.prepareStatement(sqlSelect)) {
            if(!roomCode.equals("any")) {
               ps.setString(counter++, roomCode);
            }

            if(!bedType.equals("any")) {
               ps.setString(counter++, bedType);
            }

            int totalPeople = numChildren + numAdults;
            ps.setInt(counter++, totalPeople);

            ps.setString(counter++, checkIn);
            ps.setString(counter++, checkOut);

            ps.setString(counter++, checkIn);
            ps.setString(counter++, checkOut);

            ResultSet rs = ps.executeQuery();
            while(rs.next()) {
               String rc = rs.getString("RoomCode");
               String rn = rs.getString("RoomName");
               String bt = rs.getString("BedType");
               float rate = rs.getFloat("BasePrice");
               Reservation r = new Reservation(rc, rn, bt, checkIn, checkOut, numAdults, numChildren, rate);
               ls.add(r);
            }
         }
      }
      return ls;
   }

    // Detailed Reservation Information   
    private void reservationInfo() throws SQLException {

		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("LAB6_JDBC_URL"),
									System.getenv("LAB6_JDBC_USER"),
									System.getenv("LAB6_JDBC_PW"))) {
			// Step 2: Construct SQL statement
			Search s = getSearch(); 
			String selectSql = "SELECT CODE, Room, RoomName, CheckIn, " +
								"Checkout, Rate, LastName, FirstName, Adults, Kids " +
								"FROM lab6_reservations " +
								"JOIN lab6_rooms ON (Room = RoomCode) " + 
								"WHERE FirstName LIKE ? " + 
								"AND LastName LIKE ? ";

			if (s == null) {
				return; 
			}
			if (s.getDate1() == null && s.getDate2() != null) {
				selectSql = selectSql + "AND Checkout < ? " + 
								"AND CODE LIKE ? " + 
								"AND Room LIKE ?;";
			} else if (s.getDate2() == null && s.getDate1() != null) {
				selectSql = selectSql + "AND CheckIn > ? " +
								"AND CODE LIKE ? " + 
								"AND Room LIKE ?;";
			} else if (s.getDate1() != null && s.getDate2() != null) {
				selectSql = selectSql + "AND CheckIn > ? " +
								"AND Checkout < ? " + 
								"AND CODE LIKE ? " + 
								"AND Room LIKE ?;";
			} else {
				selectSql = selectSql + "AND CODE LIKE ? " + 
								"AND Room LIKE ?;";
			}

			// Step 3: Start transaction
			conn.setAutoCommit(false);
			
			try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
			
				// Step 4: Send SQL statement to DBMS
				pstmt.setString(1, s.getFirstName());
				pstmt.setString(2, s.getLastName());

				if (s.getDate1() != null && s.getDate2() == null) {
					pstmt.setDate(3, stringToDate(s.getDate1()));
					pstmt.setString(4, s.getResCode());
					pstmt.setString(5, s.getRoomCode());
				} else if (s.getDate1() == null && s.getDate2() != null) {
					pstmt.setDate(3, stringToDate(s.getDate2()));
					pstmt.setString(4, s.getResCode());
					pstmt.setString(5, s.getRoomCode());
				} else if (s.getDate1() != null && s.getDate2() != null) {
					pstmt.setDate(3, stringToDate(s.getDate1()));
					pstmt.setDate(4, stringToDate(s.getDate2()));
					pstmt.setString(5, s.getResCode());
					pstmt.setString(6, s.getRoomCode());
				} else {
					pstmt.setString(3, s.getResCode());
					pstmt.setString(4, s.getRoomCode());
				}
				
				// Step 5: Handle results
				ResultSet rs = pstmt.executeQuery();
				ResultSetMetaData rsmd = rs.getMetaData();
				int colNum = rsmd.getColumnCount();

				for (int i = 1; i <= colNum; i++) {
					if (i == 3) {
						System.out.printf("%-30s", rsmd.getColumnName(i));
					} else {
						System.out.printf("%-12s", rsmd.getColumnName(i));
					}
				}
				System.out.println(""); 

				while (rs.next()) {
					for (int j = 1; j <= colNum; j++) {
						String colVal = rs.getString(j);
						if (j == 3) {
							System.out.printf("%-30s", colVal);
						} else {
							System.out.printf("%-12s", colVal);
						}
					}
					System.out.println(""); 
				}
				System.out.println("");

				// Step 6: Commit or rollback transaction
				conn.commit();
			} catch (SQLException e) {
				conn.rollback();
			}

		}
		// Step 7: Close connection (handled implcitly by try-with-resources syntax)
    }

	// Revenue  
    private void revenue() throws SQLException {

		// Step 1: Establish connection to RDBMS
		try (Connection conn = DriverManager.getConnection(System.getenv("LAB6_JDBC_URL"),
									System.getenv("LAB6_JDBC_USER"),
									System.getenv("LAB6_JDBC_PW"))) {
			// Step 2: Construct SQL statement

			String dropTable = "DROP VIEW IF EXISTS Revenue;";
			String createView = "CREATE VIEW Revenue AS " + 
				"SELECT Room, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 01 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS January, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 02 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS February, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 03 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS March, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 04 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS April, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 05 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS May, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 06 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS June, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 07 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS July, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 08 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS August, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 09 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS September, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 10 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS October, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 11 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS November, " + 
				"ROUND(SUM(CASE WHEN Month(Checkout) = 12 AND Year(Checkout) = Year(Curdate()) THEN Rate ELSE 0.0 END), 2) AS December, " + 
				"ROUND(SUM(Rate), 2) AS TotalRevenue " + 
				"FROM lab6_reservations " + 
				"GROUP BY Room;";
			String selectSql = "SELECT * FROM Revenue " + 
				"UNION ALL " + 
				"SELECT 'Total', SUM(January) January, SUM(February) February, SUM(March) March, " + 
					"SUM(April) April, SUM(May) May, SUM(June) June, SUM(July) July, " +
					"SUM(August) August, SUM(September) September, SUM(October) October, " +
					"SUM(November) November, SUM(December) December, SUM(TotalRevenue) TotalRevenue " +
				"FROM Revenue;";

			// Step 3: Start transaction
			conn.setAutoCommit(false);
			
			try (Statement stmt = conn.createStatement()) {
				
				// Step 4: Handle results
				stmt.execute(dropTable);
				stmt.execute(createView);
				ResultSet rs = stmt.executeQuery(selectSql);
				ResultSetMetaData rsmd = rs.getMetaData();
				int colNum = rsmd.getColumnCount();

				for (int i = 1; i <= colNum; i++) {
					System.out.printf("%-12s", rsmd.getColumnName(i));
				}
				System.out.println(""); 

				while (rs.next()) {
					for (int j = 1; j <= colNum; j++) {
						String colVal = rs.getString(j);
						System.out.printf("%-12s", colVal);
					}
					System.out.println(""); 
				}
				System.out.println("");

				// Step 5: Commit or rollback transaction
				conn.commit();
			} catch (SQLException e) {
				conn.rollback();
			}

		}
		// Step 6: Close connection (handled implcitly by try-with-resources syntax)
    }

	private java.sql.Date stringToDate(String dateString) {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		try {
			java.util.Date date = format.parse(dateString);
			return new java.sql.Date(date.getTime());
		} catch(ParseException e) {
			return null; 
		}
	}

	private Search getSearch() {
		Scanner scanner = new Scanner(System.in);
		Search s = new Search();
		String resp; 
	
		System.out.print("Enter first name (Enter to skip or 'Q' to quit): ");
		resp = scanner.nextLine();
		if (resp.equals("Q")) {
			return null; 
		}
		if (!resp.isEmpty()) {
			s.setEntry(FN, resp);
		}

		System.out.print("Enter last name (Enter to skip or 'Q' to quit): ");
		resp = scanner.nextLine(); 
		if (resp.equals("Q")) {
			return null; 
		}
		if (!resp.isEmpty()) {
			s.setEntry(LN, resp);
		}

		System.out.print("Enter date range [YYYY-MM-DD - YYYY-MM-DD] " +
							"(Enter to skip or 'Q' to quit): "); 
		resp = scanner.nextLine(); 
		if (resp.equals("Q")) {
			return null; 
		}
		if (!resp.isEmpty()) {
			String[] respSplit = resp.split(" "); 
			if (respSplit.length == 3) {
				String dateString1 = respSplit[0];
				String dateString2 = respSplit[2];

				s.setEntry(D1, dateString1);
				s.setEntry(D2, dateString2);
			} else {
				System.out.println("Wrong date format! Quitting now.");
				return null; 
			}
		}

		System.out.print("Enter room code (Enter to skip or 'Q' to quit): ");
		resp = scanner.nextLine();
		if (resp.equals("Q")) {
			return null; 
		}
		if (!resp.isEmpty()) {
			s.setEntry(RMC, resp);
		} 

		System.out.print("Enter reservation code (Enter to skip or 'Q' to quit): ");
		resp = scanner.nextLine(); 
		if (resp.equals("Q")) {
			return null; 
		}
		if (!resp.isEmpty()) {
			s.setEntry(RSC, resp);
		}
		
		return s; 
	}

	private class Search {
		String firstName, lastName, date1, date2, roomCode, reservationCode; 

		public Search() {
			this.firstName = "%";
			this.lastName = "%"; 
			this.date1 = null;
			this.date2 = null; 
			this.roomCode = "%";
			this.reservationCode = "%";
		}

		public String getFirstName() { return firstName; }
		public String getLastName() { return lastName; }
		public String getDate1() { return date1; }
		public String getDate2() { return date2; }
		public String getRoomCode() { return roomCode; }
		public String getResCode() { return reservationCode; }
		public void setEntry(int entry, String newEntry) { 
			if (entry == FN) {
				firstName = newEntry;
			} else if (entry == LN) {
				lastName = newEntry;
			} else if (entry == D1) {
				date1 = newEntry; 
			} else if (entry == D2) {
				date2 = newEntry; 
			} else if (entry == RMC) {
				roomCode = newEntry;
			} else if (entry == RSC) {
				reservationCode = newEntry;
			}
		}

	}
       
    private class Reservation {
      private String roomCode, roomName, bedType, checkIn, checkOut;
      private int numAdults, numChildren;
      private float rate;

      public Reservation(String roomCode, String roomName,
                        String bedType, String checkIn, String checkOut,
                        int numAdults, int numChildren, float rate) {
         this.roomCode = roomCode;
         this.roomName = roomName;
         this.bedType = bedType;
         this.checkIn = checkIn;
         this.checkOut = checkOut;
         this.numAdults = numAdults;
         this.numChildren = numChildren;
         this.rate = rate;
      }

      public String getRoomCode() {return this.roomCode;}
      public String getRoomName() {return this.roomName;}
      public String getBedType() {return this.bedType;}
      public String getCheckIn() {return this.checkIn;}
      public String getCheckOut() {return this.checkOut;}
      public int getNumAdults() {return this.numAdults;}
      public int getNumChildren() {return this.numChildren;}
      public float getRate() {return this.rate;}
   }
}
