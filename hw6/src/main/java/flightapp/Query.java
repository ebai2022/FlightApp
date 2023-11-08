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
          int result_time = oneHopResults.getInt("actual_time");
          // Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n [first flight in itinerary]\n ... [last flight in itinerary]\n
          sb.append("Itinerary " + itinerary_number + ": 1 flight(s), " + result_time + " minutes\n");
          sb.append(print_direct(oneHopResults));
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
        twoHopResults.close();

        // merge the results and print them
        int i = 0;
        int j = 0;
        while (i < direct_results.size() && j < indirect_results.size()){
          ResultSet direct_result = direct_results.get(i);
          ResultSet indirect_result = indirect_results.get(j);
          int direct_time = direct_result.getInt("actual_time");
          int indirect_time = indirect_result.getInt("actual_time1") + indirect_result.getInt("actual_time2");
          // more equality cases
          if (compare(direct_result, indirect_result)){
            int result_time = direct_result.getInt("actual_time");
            sb.append("Itinerary " + itinerary_number + ": 1 flight(s), " + result_time + " minutes\n");
            sb.append(print_direct(direct_result));
            i++;
          } else {
            int result_time1 = indirect_result.getInt("actual_time1");
            int result_time2 = indirect_result.getInt("actual_time2");
            sb.append("Itinerary " + itinerary_number + ": 2 flight(s), " + (result_time1 + result_time2) + " minutes\n");
            sb.append(print_indirect(indirect_result));
            j++;
          }
          itinerary_number++;
        }

        /*
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
        */
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
   * rs1 is guaranteed to be a direct flight, rs2 is guaranteed to be an indirect flight
   * @Returns True if rs1 comes first, false otherwise
   */
  private boolean compare(ResultSet rs1, ResultSet rs2){
    try {
      int direct_time = rs1.getInt("actual_time");
      int indirect_time = rs2.getInt("actual_time1") + rs2.getInt("actual_time2");
      if (direct_time < indirect_time){
        return true;
      } 
      else if (direct_time > indirect_time){
        return false;
      } else {
        int fid1 = rs2.getInt("fid1");
        int fid2 = rs2.getInt("fid2");
        if (fid1 <= fid2){
          return true;
        } else {
          return false;
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  private String print_direct(ResultSet direct_result){
    try {
      int result_dayOfMonth = direct_result.getInt("day_of_month");
      String fid = direct_result.getString("fid");
      String result_carrierId = direct_result.getString("carrier_id");
      String result_flightNum = direct_result.getString("flight_num");
      String result_originCity = direct_result.getString("origin_city");
      String result_destCity = direct_result.getString("dest_city");
      int result_time = direct_result.getInt("actual_time");
      int result_capacity = direct_result.getInt("capacity");
      int result_price = direct_result.getInt("price");
      return "ID: " + fid + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " 
          + result_flightNum + " Origin: " + result_originCity + " Dest: " + result_destCity + " Duration: " 
          + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n";
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "";
  }

  private String print_indirect(ResultSet indirect_result){
    try{
      int result_dayOfMonth = indirect_result.getInt("day_of_month");
      String fid1 = indirect_result.getString("fid1");
      String fid2 = indirect_result.getString("fid2");
      String result_carrierId1 = indirect_result.getString("carrier_id1");
      String result_carrierId2 = indirect_result.getString("carrier_id2");
      String result_flightNum1 = indirect_result.getString("flight_num1");
      String result_flightNum2 = indirect_result.getString("flight_num2");
      String result_originCity1 = indirect_result.getString("origin_city1");
      String result_originCity2 = indirect_result.getString("origin_city2");
      String result_destCity1 = indirect_result.getString("dest_city1");
      String result_destCity2 = indirect_result.getString("dest_city2");
      int result_capacity1 = indirect_result.getInt("capacity1");
      int result_capacity2 = indirect_result.getInt("capacity2");
      int result_time1 = indirect_result.getInt("actual_time1");
      int result_time2 = indirect_result.getInt("actual_time2");
      int result_price1 = indirect_result.getInt("price1");
      int result_price2 = indirect_result.getInt("price2");
      String p1 = "ID: " + fid1 + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId1 + " Number: " 
            + result_flightNum1 + " Origin: " + result_originCity1 + " Dest: " + result_destCity1 + " Duration: " 
            + result_time1 + " Capacity: " + result_capacity1 + " Price: " + result_price1 + "\n";
      String p2 = "ID: " + fid2 + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId2 + " Number: " 
            + result_flightNum2 + " Origin: " + result_originCity2 + " Dest: " + result_destCity2 + " Duration: " 
            + result_time2 + " Capacity: " + result_capacity2 + " Price: " + result_price2 + "\n";
      return p1 + p2;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return "";
  }
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
