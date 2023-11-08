package flightapp;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.*;

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
  private static final String get_direct_flights = "SELECT TOP (?) * FROM FLIGHTS F, "
  + "CARRIERS C WHERE F.carrier_id = C.cid AND origin_city = ? AND dest_city = ? "
  + "AND day_of_month = ? AND canceled = 0 ORDER BY actual_time, fid";
  // is it possible for a flight to go from dest 1 back to dest 1 and then to dest 2?
  private static final String get_onestop_flights = "SELECT TOP (?) F1.fid, F2.fid, F1.carrier_id, "
  + "F2.carrier_id, F1.flight_num, F2.flight_num, F1.origin_city, F2.origin_city, F1.dest_city, "
  + "F2.dest_city, F1.actual_time, F2.actual_time, F1.capacity, F2.capacity, F1.price, F2.price "
  + "FROM FLIGHTS F1, FLIGHTS F2 WHERE F1.origin_city = ? AND F1.dest_city = F2.origin_city "
  + "AND F2.dest_city = ? AND F1.day_of_month = ? AND F2.day_of_month = ? AND F1.canceled = 0 "
  + "AND F2.canceled = 0 ORDER BY F1.actual_time + F2.actual_time, F1.fid, F2.fid";
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
  private List<Routes> routes;

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
      /*
       * Check the amount of direct flights. If we are only asking for direct flights, return that string buffer.
       * Check if we need more flights to reach the number of itineraries. If not, return the same string buffer as previous.
       * If so, get indirect flights and merge the two string buffers.
       */
      routes = new ArrayList<Routes>();
      // get all direct flights
      get_direct_flights_stmt.clearParameters();
      get_direct_flights_stmt.setInt(1, numberOfItineraries);
      get_direct_flights_stmt.setString(2, originCity);
      get_direct_flights_stmt.setString(3, destinationCity);
      get_direct_flights_stmt.setInt(4, dayOfMonth);
      ResultSet oneHopResults = get_direct_flights_stmt.executeQuery();
      while (oneHopResults.next()) {
        int fid = oneHopResults.getInt("fid");
        int day = oneHopResults.getInt("day_of_month");
        String carrierId = oneHopResults.getString("carrier_id");
        String flightNum = oneHopResults.getString("flight_num");
        String flight_origin = oneHopResults.getString("origin_city");
        String flight_dest = oneHopResults.getString("dest_city");
        int time = oneHopResults.getInt("actual_time");
        int capacity = oneHopResults.getInt("capacity");
        int price = oneHopResults.getInt("price");
        Flight f = new Flight(fid, day, carrierId, flightNum, flight_origin, flight_dest, time, capacity, price);
        List<Flight> flights = new ArrayList<Flight>();
        flights.add(f);
        Routes r = new Routes(0, time, day, price, flights, flight_origin, flight_dest);
        routes.add(r);
      }
      oneHopResults.close();
      
      if (!directFlight && routes.size() < numberOfItineraries){
        // If we have gotten to this point, we know we need to fulfill the rest of the flights with indirect flights
        get_onestop_flights_stmt.clearParameters();
        get_onestop_flights_stmt.setInt(1, numberOfItineraries - routes.size());
        get_onestop_flights_stmt.setString(2, originCity);
        get_onestop_flights_stmt.setString(3, destinationCity);
        get_onestop_flights_stmt.setInt(4, dayOfMonth);
        get_onestop_flights_stmt.setInt(5, dayOfMonth);
        ResultSet twoHopResults = get_onestop_flights_stmt.executeQuery();
        while (twoHopResults.next()){
          int fid1 = twoHopResults.getInt(1);
          int fid2 = twoHopResults.getInt(2);
          String carrierId1 = twoHopResults.getString(3);
          String carrierId2 = twoHopResults.getString(4);
          String flightNum1 = twoHopResults.getString(5);
          String flightNum2 = twoHopResults.getString(6);
          String flight_origin1 = twoHopResults.getString(7);
          String flight_origin2 = twoHopResults.getString(8);
          String flight_dest1 = twoHopResults.getString(9);
          String flight_dest2 = twoHopResults.getString(10);
          int time1 = twoHopResults.getInt(11);
          int time2 = twoHopResults.getInt(12);
          int capacity1 = twoHopResults.getInt(13);
          int capacity2 = twoHopResults.getInt(14);
          int price1 = twoHopResults.getInt(15);
          int price2 = twoHopResults.getInt(16);
          int day = dayOfMonth;
          Flight f1 = new Flight(fid1, day, carrierId1, flightNum1, flight_origin1, flight_dest1, time1, capacity1, price1);
          Flight f2 = new Flight(fid2, day, carrierId2, flightNum2, flight_origin2, flight_dest2, time2, capacity2, price2);
          List<Flight> flights = new ArrayList<Flight>();
          flights.add(f1);
          flights.add(f2);
          Routes r = new Routes(0, time1 + time2, day, price1 + price2, flights, flight_origin1, flight_dest2);
          routes.add(r);
        }
        twoHopResults.close();
      }
      // merge the results and print them
    } catch (SQLException e) {
      e.printStackTrace();
      return "Failed to search\n";
    }
    if (routes.size() == 0) {
      return "No flights match your selection\n";
    }
    Collections.sort(routes);
    for (int i = 0; i < routes.size(); i++) {
      Routes curr = routes.get(i);
      curr.route_num = i;
      sb.append(curr);
    }
    return sb.toString();
  }

  class Routes implements Comparable<Routes>{
    public int route_num;
    public int time;
    public int date;
    public int price;
    public List<Flight> flights;
    public String origin_city;
    public String dest_city;

    public Routes(int route_num, int time, int date, int price, List<Flight> flights, String origin_city, String dest_city) {
      this.route_num = route_num;
      this.time = time;
      this.date = date;
      this.price = price;
      this.flights = flights;
      this.origin_city = origin_city;
      this.dest_city = dest_city;
    }

    @Override
    public int compareTo(Routes r2) {
      if (this.time == r2.time) {
        if (this.flights.get(0).fid == r2.flights.get(0).fid) {
          return this.flights.get(1).fid - r2.flights.get(1).fid;
        }
        return this.flights.get(0).fid - r2.flights.get(0).fid;
      }
      return this.time - r2.time;
    }

    @Override
    public String toString() {
      StringBuilder str = new StringBuilder();
      str.append("Itinerary " + this.route_num + ": " + flights.size() + " flight(s), " + this.time + " minutes\n");
      for (Flight f : flights) {
        str.append(f);
        str.append("\n");
      }
      return str.toString();
    }
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
