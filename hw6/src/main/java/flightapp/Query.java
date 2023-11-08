package flightapp;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Runs queries against a back-end database
 */
public class Query extends QueryAbstract {
  //
  // Canned queries
  //
  private static final String FLIGHT_CAPACITY_SQL = "SELECT capacity FROM Flights WHERE fid = ?";
  private static final String check_user_exists = "SELECT * FROM Users_ebai2022 WHERE username = ?";
  private static final String clear_users = "DELETE FROM Users_ebai2022";
  private static final String clear_reservations = "DELETE FROM RESERVATIONS_ebai2022";
  private static final String create_customer = "INSERT INTO Users_ebai2022 VALUES (?, ?, ?)";
  private static final String get_password = "SELECT password FROM Users_ebai2022 WHERE username = ?";

  // breaking ties by actual_time, then fid1, then fid2
  private static final String get_direct_flights = "SELECT TOP (?) * FROM FLIGHTS F, CARRIERS C WHERE F.carrier_id = C.cid AND origin_city = ? AND dest_city = ? AND day_of_month = ? AND canceled = 0 ORDER BY actual_time, fid";
  // is it possible for a flight to go from dest 1 back to dest 1 and then to dest 2?
  private static final String get_onestop_flights = "SELECT TOP (?) * FROM FLIGHTS F1, FLIGHTS F2 WHERE F1.origin_city = ? AND F1.dest_city = F2.origin_city AND F2.dest_city = ? AND F1.day_of_month = ? AND F2.day_of_month = ? AND F1.canceled = 0 AND F2.canceled = 0 ORDER BY F1.actual_time + F2.actual_time, F1.fid, F2.fid";
  //
  // Instance variables
  //
  private PreparedStatement flightCapacityStmt;
  private PreparedStatement check_user_exists_stmt;
  private PreparedStatement clear_users_stmt;
  private PreparedStatement clear_reservations_stmt;
  private PreparedStatement create_customer_stmt; 
  private PreparedStatement get_password_stmt;
  private PreparedStatement get_direct_flights_stmt;
  private PreparedStatement get_onestop_flights_stmt;
  private boolean isLoggedIn;

  protected Query() throws SQLException, IOException {
    prepareStatements();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      // TODO: YOUR CODE HERE
      clear_users_stmt.executeUpdate();
      clear_reservations_stmt.executeUpdate();
      // System.out.println("Cleared " + users_cleared + " users and " + reservations_cleared + " reservations");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    flightCapacityStmt = conn.prepareStatement(FLIGHT_CAPACITY_SQL);
    // TODO: YOUR CODE HERE
    check_user_exists_stmt = conn.prepareStatement(check_user_exists);
    clear_users_stmt = conn.prepareStatement(clear_users);
    clear_reservations_stmt = conn.prepareStatement(clear_reservations);
    create_customer_stmt = conn.prepareStatement(create_customer);
    get_password_stmt = conn.prepareStatement(get_password);
    get_direct_flights_stmt = conn.prepareStatement(get_direct_flights);
    get_onestop_flights_stmt = conn.prepareStatement(get_onestop_flights);
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_login(String username, String password) {
    // TODO: YOUR CODE HERE
    if (isLoggedIn) {
      return "User already logged in\n";
    }
    try {
      // Check if the password matches the stored password
      get_password_stmt.clearParameters();
      get_password_stmt.setString(1, username);
      ResultSet rs = get_password_stmt.executeQuery();
      // I assume if the user is not found, rs.next() will be false and not null 
      // so the login will fail - double check this assumption
      while (rs.next()) {
        byte[] saltedHash = rs.getBytes("password");
        if (PasswordUtils.plaintextMatchesSaltedHash(password, saltedHash)) {
          isLoggedIn = true;
          rs.close();
          return "Logged in as " + username + "\n";
        }
      }
      rs.close();
    }
    catch (SQLException e) {
      e.printStackTrace();
    }
    return "Login failed\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    // TODO: YOUR CODE HERE
    // Do I need to check NULLS and other exceptions?
    // Fail to create user if they have a negative balance
    if (initAmount < 0) {
      return "Failed to create user\n";
    }
    // Usernames are not case-sensitive, so convert to lowercase
    username = username.toLowerCase();
    try {
      // Fail to create user if that username already exists
      check_user_exists_stmt.clearParameters();
      check_user_exists_stmt.setString(1, username);
      ResultSet rs = check_user_exists_stmt.executeQuery();
      while (rs.next()) {
        rs.close();
        return "Failed to create user\n";
      }
      rs.close();
      // Create the user
      create_customer_stmt.clearParameters();
      create_customer_stmt.setString(1, username);
      byte[] saltedHash = PasswordUtils.saltAndHashPassword(password);
      create_customer_stmt.setBytes(2, saltedHash);
      create_customer_stmt.setInt(3, initAmount);
      create_customer_stmt.executeUpdate();
      return "Created user " + username + "\n";
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "Failed to create user\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_search(String originCity, String destinationCity, 
                                   boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries) {
    // WARNING: the below code is insecure (it's susceptible to SQL injection attacks) AND only
    // handles searches for direct flights.  We are providing it *only* as an example of how
    // to use JDBC; you are required to replace it with your own secure implementation.
    //
    // TODO: YOUR CODE HERE

    StringBuffer sb = new StringBuffer();
    try {
      int itinerary_number = 0;
      // get all direct flights
      if (directFlight){
        get_direct_flights_stmt.clearParameters();
        get_direct_flights_stmt.setInt(1, numberOfItineraries);
        get_direct_flights_stmt.setString(2, originCity);
        get_direct_flights_stmt.setString(3, destinationCity);
        get_direct_flights_stmt.setInt(4, dayOfMonth);
        ResultSet oneHopResults = get_direct_flights_stmt.executeQuery();

        while (oneHopResults.next()) {
          int result_dayOfMonth = oneHopResults.getInt("day_of_month");
          String fid = oneHopResults.getString("fid");
          String result_carrierId = oneHopResults.getString("carrier_id");
          String result_flightNum = oneHopResults.getString("flight_num");
          String result_originCity = oneHopResults.getString("origin_city");
          String result_destCity = oneHopResults.getString("dest_city");
          int result_time = oneHopResults.getInt("actual_time");
          int result_capacity = oneHopResults.getInt("capacity");
          int result_price = oneHopResults.getInt("price");
          // Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n [first flight in itinerary]\n ... [last flight in itinerary]\n
          sb.append("Itinerary " + itinerary_number + ": 1 flight(s), " + result_time + " minutes\n" + "ID: " 
          + fid + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " 
          + result_flightNum + " Origin: " + result_originCity + " Dest: " + result_destCity + " Duration: " 
          + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
          itinerary_number++;
        }
        oneHopResults.close();
      } else {
        // get all direct flights
        get_direct_flights_stmt.clearParameters();
        get_direct_flights_stmt.setInt(1, numberOfItineraries);
        get_direct_flights_stmt.setString(2, originCity);
        get_direct_flights_stmt.setString(3, destinationCity);
        get_direct_flights_stmt.setInt(4, dayOfMonth);
        ResultSet oneHopResults = get_direct_flights_stmt.executeQuery();
        ArrayList<ResultSet> direct_results = new ArrayList<ResultSet>();
        while (oneHopResults.next()) {
          direct_results.add(oneHopResults);
        }
        oneHopResults.close();

        // get all indirect flights
        get_onestop_flights_stmt.clearParameters();
        get_onestop_flights_stmt.setInt(1, numberOfItineraries - direct_results.size());
        get_onestop_flights_stmt.setString(2, originCity);
        get_onestop_flights_stmt.setString(3, destinationCity);
        get_onestop_flights_stmt.setInt(4, dayOfMonth);
        get_onestop_flights_stmt.setInt(5, dayOfMonth);
        ResultSet twoHopResults = get_onestop_flights_stmt.executeQuery();
        ArrayList<ResultSet> indirect_results = new ArrayList<ResultSet>();
        while (twoHopResults.next()) {
          indirect_results.add(twoHopResults);
        }
        // Sort the results
        /*
         * results.sort((rs1, rs2) -> {
          try {
            // sort by total time
            int time1 = rs1.getInt("actual_time");
            int time2 = rs2.getInt("actual_time");
            if (time1 != time2) {
              return time1 - time2;
            }
            // sort by fid1
            String fid1 = rs1.getString("fid1");
            String fid2 = rs2.getString("fid1");
            if (!fid1.equals(fid2)) {
              return fid1.compareTo(fid2);
            }
            // sort by fid2
            fid1 = rs1.getString("fid2");
            fid2 = rs2.getString("fid2");
            if (!fid1.equals(fid2)) {
              return fid1.compareTo(fid2);
            }
          } catch (SQLException e) {
            e.printStackTrace();
            return 0;
          }
        });
         */
        int result_dayOfMonth = twoHopResults.getInt("day_of_month");
        String fid1 = twoHopResults.getString("fid1");
        String fid2 = twoHopResults.getString("fid2");
        String result_carrierId1 = twoHopResults.getString("carrier_id1");
        String result_carrierId2 = twoHopResults.getString("carrier_id2");
        String result_flightNum1 = twoHopResults.getString("flight_num1");
        String result_flightNum2 = twoHopResults.getString("flight_num2");
        String result_originCity1 = twoHopResults.getString("origin_city1");
        String result_originCity2 = twoHopResults.getString("origin_city2");
        String result_destCity1 = twoHopResults.getString("dest_city1");
        String result_destCity2 = twoHopResults.getString("dest_city2");
        int result_capacity1 = twoHopResults.getInt("capacity1");
        int result_capacity2 = twoHopResults.getInt("capacity2");
        int result_time1 = twoHopResults.getInt("actual_time1");
        int result_time2 = twoHopResults.getInt("actual_time2");
        int result_price1 = twoHopResults.getInt("price1");
        int result_price2 = twoHopResults.getInt("price2");
        sb.append("Itinerary " + itinerary_number + ": 2 flight(s), " + (result_time1 + result_time2) + " minutes\n");
        sb.append("ID: " + fid1 + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId1 + " Number: " 
        + result_flightNum1 + " Origin: " + result_originCity1 + " Dest: " + result_destCity1 + " Duration: " 
        + result_time1 + " Capacity: " + result_capacity1 + " Price: " + result_price1 + "\n");
        sb.append("ID: " + fid2 + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId2 + " Number: " 
        + result_flightNum2 + " Origin: " + result_originCity2 + " Dest: " + result_destCity2 + " Duration: " 
        + result_time2 + " Capacity: " + result_capacity2 + " Price: " + result_price2 + "\n");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return "Failed to search\n";
    }
    if (sb.length() == 0) {
      return "No flights match your selection\n";
    }
    return sb.toString();
  }

  /*
   * Override comparable class for ResultSets
  */

  /*class ResultSetComparator implements Comparable<ResultSetComparator> {
    public ResultSet rs;
    public int dayOfMonth;
    public String fid1;
    public String fid2;
    public String result_carrierId1;
    public String result_carrierId2;
    public String result_flightNum1;
    public String result_flightNum2;
    public String result_originCity1;
    public String result_originCity2;
    public String result_destCity1;
    public String result_destCity2;
    public int result_capacity1;
    public int result_capacity2;
    public int result_time1;
    public int result_time2;
    public int result_price1;
    public int result_price2;

    ResultSetComparator(ResultSet rs) {
      this.rs = rs;
      try {
        this.dayOfMonth = rs.getInt("day_of_month");
        this.fid1 = rs.getString("fid1");
        this.fid2 = rs.getString("fid2");
        this.result_carrierId1 = rs.getString("carrier_id1");
        this.result_carrierId2 = rs.getString("carrier_id2");
        this.result_flightNum1 = rs.getString("flight_num1");
        this.result_flightNum2 = rs.getString("flight_num2");
        this.result_originCity1 = rs.getString("origin_city1");
        this.result_originCity2 = rs.getString("origin_city2");
        this.result_destCity1 = rs.getString("dest_city1");
        this.result_destCity2 = rs.getString("dest_city2");
        this.result_capacity1 = rs.getInt("capacity1");
        this.result_capacity2 = rs.getInt("capacity2");
        this.result_time1 = rs.getInt("actual_time1");
        this.result_time2 = rs.getInt("actual_time2");
        this.result_price1 = rs.getInt("price1");
        this.result_price2 = rs.getInt("price2");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    @Override
    public int compareTo(ResultSetComparator other) {
      return 0;
    }
  }*/
  
  /* See QueryAbstract.java for javadoc */
  public String transaction_book(int itineraryId) {
    // TODO: YOUR CODE HERE
    return "Booking failed\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_pay(int reservationId) {
    // TODO: YOUR CODE HERE
    return "Failed to pay for reservation " + reservationId + "\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_reservations() {
    // TODO: YOUR CODE HERE
    return "Failed to retrieve reservations\n";
  }

  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    flightCapacityStmt.clearParameters();
    flightCapacityStmt.setInt(1, fid);

    ResultSet results = flightCapacityStmt.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }

  /**
   * Utility function to determine whether an error was caused by a deadlock
   */
  private static boolean isDeadlock(SQLException e) {
    return e.getErrorCode() == 1205;
  }

  /**
   * A class to store information about a single flight
   *
   * TODO(hctang): move this into QueryAbstract
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    Flight(int id, int day, String carrier, String fnum, String origin, String dest, int tm,
           int cap, int pri) {
      fid = id;
      dayOfMonth = day;
      carrierId = carrier;
      flightNum = fnum;
      originCity = origin;
      destCity = dest;
      time = tm;
      capacity = cap;
      price = pri;
    }
    
    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: "
          + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time
          + " Capacity: " + capacity + " Price: " + price;
    }
  }
}
