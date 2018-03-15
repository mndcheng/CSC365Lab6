import java.time.LocalDate;
import java.util.*;
import java.sql.*;
import java.text.*;

/*
-- Shell init:
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
			System.out.print("Reservation Info or Revenue or Quit: ");
			InnReservations innRes = new InnReservations();
			
			while (!(sc.hasNext("Quit"))) {
				String input = sc.nextLine();
				input = input.toLowerCase();
				System.out.println("");

				if (input.equals("reservation info")) {
					innRes.reservationInfo();
				} else if (input.equals("revenue")) {
					innRes.revenue();
				}

				System.out.print("Reservation Info or Revenue or Quit: ");
			}
			
		} catch (SQLException e) {
			System.err.println("SQLException: " + e.getMessage());
		}
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
		int fn, ln, d1, d2, rmC, rsC;
		String firstName, lastName, date1, date2, roomCode, reservationCode; 

		public Search() {
			this.fn = fn;
			this.ln = ln;
			this.d1 = d1;
			this.d2 = d2;
			this.rmC = rmC;
			this.rsC = rsC;
			this.firstName = "%";
			this.lastName = "%"; 
			this.date1 = null;
			this.date2 = null; 
			this.roomCode = "%";
			this.reservationCode = "%";
		}

		public void printValues() {
			System.out.println("first name: " + firstName);
			System.out.println("last name: " + lastName);
			System.out.print("checkIn: " + date1 +
							"\n checkout: " + date2 + 
							"\n room code: " + roomCode + 
							"\n reservation code: " + reservationCode + "\n"); 
		}

		public int getFn() { return fn; }

		public int getLn() { return ln; }

		public int getD1() { return d1; }

		public int getD2() { return d2; }

		public int getRmC() { return rmC; }

		public int getRsC() { return rsC; }

		public String getFirstName() { return firstName; }

		public String getLastName() { return lastName; }

		public String getDate1() { return date1; }

		public String getDate2() { return date2; }

		public String getRoomCode() { return roomCode; }

		public String getResCode() { return reservationCode; }

		public void setField(int newField) {
			if (newField == FN) {
				fn = newField;
			} else if (newField == LN) {
				ln = newField;
			} else if (newField == D1) {
				d1 = newField;
			} else if (newField == D2) {
				d2 = newField; 
			} else if (newField == RMC) {
				rmC = newField; 
			} else if (newField == RSC) {
				rsC = newField;
			}
		}

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

}