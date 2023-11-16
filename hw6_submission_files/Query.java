package flightapp;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.microsoft.sqlserver.jdbc.SQLServerError;

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
  private static final String check_same_day_reservation = "SELECT * FROM RESERVATIONS_ebai2022 R, FLIGHTS F "
  + "WHERE R.fid1 = F.fid AND username = ? AND F.day_of_month = ?";
  private static final String get_curr_rid_max = "SELECT MAX(rid) FROM RESERVATIONS_ebai2022";
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
  private static final String book_reservation = "INSERT INTO RESERVATIONS_ebai2022 VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  private static final String check_reservation_status = "SELECT COUNT(*) FROM RESERVATIONS_ebai2022 WHERE rid = ? AND username = ? AND paid = 0";
  private static final String get_reservation = "SELECT * FROM RESERVATIONS_ebai2022 WHERE rid = ? AND username = ?";
  private static final String get_user_reservation = "SELECT * FROM RESERVATIONS_ebai2022 WHERE username = ?";
  private static final String update_paid = "UPDATE RESERVATIONS_ebai2022 SET paid = 1 WHERE rid = ? AND username = ?";
  private static final String update_user_balance = "UPDATE Users_ebai2022 SET balance = ? WHERE username = ?";
  private static final String get_flight = "SELECT fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price "
  + "FROM FLIGHTS WHERE fid = ?";
  private static final String get_num_reserved = "SELECT COUNT(*) FROM RESERVATIONS_ebai2022 WHERE fid1 = ? OR fid2 = ?";
  //
  // Instance variables
  //
  private PreparedStatement flightCapacityStmt;
  private PreparedStatement check_user_exists_stmt;
  private PreparedStatement clear_users_stmt;
  private PreparedStatement clear_reservations_stmt;
  private PreparedStatement create_customer_stmt; 
  private PreparedStatement get_password_stmt;
  private PreparedStatement check_same_day_reservation_stmt;
  private PreparedStatement get_curr_rid_max_stmt;
  private PreparedStatement get_direct_flights_stmt;
  private PreparedStatement get_onestop_flights_stmt;
  private PreparedStatement book_reservation_stmt;
  private PreparedStatement check_reservation_status_stmt;
  private PreparedStatement get_reservation_stmt;
  private PreparedStatement get_user_reservation_stmt;
  private PreparedStatement update_paid_stmt;
  private PreparedStatement update_user_balance_stmt;
  private PreparedStatement get_flight_stmt;
  private PreparedStatement get_num_reserved_stmt;
  private boolean isLoggedIn;
  private String username;
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
    check_same_day_reservation_stmt = conn.prepareStatement(check_same_day_reservation);
    get_curr_rid_max_stmt = conn.prepareStatement(get_curr_rid_max);
    get_direct_flights_stmt = conn.prepareStatement(get_direct_flights);
    get_onestop_flights_stmt = conn.prepareStatement(get_onestop_flights);
    book_reservation_stmt = conn.prepareStatement(book_reservation);
    check_reservation_status_stmt = conn.prepareStatement(check_reservation_status);
    get_reservation_stmt = conn.prepareStatement(get_reservation);
    get_user_reservation_stmt = conn.prepareStatement(get_user_reservation);
    update_paid_stmt = conn.prepareStatement(update_paid);
    update_user_balance_stmt = conn.prepareStatement(update_user_balance);
    get_flight_stmt = conn.prepareStatement(get_flight);
    get_num_reserved_stmt = conn.prepareStatement(get_num_reserved);
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
      while (rs.next()) {
        byte[] saltedHash = rs.getBytes("password");
        if (PasswordUtils.plaintextMatchesSaltedHash(password, saltedHash)) {
          isLoggedIn = true;
          rs.close();
          this.username = username.toLowerCase();
          routes = null;
          return "Logged in as " + username + "\n";
        }
      }
      rs.close();
    }
    catch (SQLException e) {
      e.printStackTrace();
    } finally {
      checkDanglingTransaction();
    }
    return "Login failed\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    // TODO: YOUR CODE HERE
    // Fail to create user if they have a negative balance
    if (initAmount < 0) {
      return "Failed to create user\n";
    }
    // Usernames are not case-sensitive, so convert to lowercase
    username = username.toLowerCase();
    boolean lock = true;
    try {
      while (lock){
        try {
          conn.setAutoCommit(false);
          // Fail to create user if that username already exists
          check_user_exists_stmt.clearParameters();
          check_user_exists_stmt.setString(1, username);
          ResultSet rs = check_user_exists_stmt.executeQuery();
          while (rs.next()) {
            rs.close();
            conn.rollback();
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
          conn.commit();
          return "Created user " + username + "\n";
        } catch (SQLException e) {
          lock = isDeadlock(e);
          conn.rollback();
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      checkDanglingTransaction();
    }
    return "Failed to create user\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_search(String originCity, String destinationCity, 
                                   boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries) {
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
      ResultSet directResults = get_direct_flights_stmt.executeQuery();
      while (directResults.next()) {
        int fid = directResults.getInt("fid");
        int day = directResults.getInt("day_of_month");
        String flight_origin = directResults.getString("origin_city");
        String flight_dest = directResults.getString("dest_city");
        int time = directResults.getInt("actual_time");
        int price = directResults.getInt("price");
        Flight f = getFlight(fid);
        List<Flight> flights = new ArrayList<Flight>();
        flights.add(f);
        Routes r = new Routes(0, time, day, price, flights, flight_origin, flight_dest);
        routes.add(r);
      }
      directResults.close();
      
      if (!directFlight && routes.size() < numberOfItineraries){
        // If we have gotten to this point, we know we need to fulfill the rest of the flights with indirect flights
        get_onestop_flights_stmt.clearParameters();
        get_onestop_flights_stmt.setInt(1, numberOfItineraries - routes.size());
        get_onestop_flights_stmt.setString(2, originCity);
        get_onestop_flights_stmt.setString(3, destinationCity);
        get_onestop_flights_stmt.setInt(4, dayOfMonth);
        get_onestop_flights_stmt.setInt(5, dayOfMonth);
        ResultSet oneStopResults = get_onestop_flights_stmt.executeQuery();
        while (oneStopResults.next()){
          int fid1 = oneStopResults.getInt(1);
          int fid2 = oneStopResults.getInt(2);
          String flight_origin1 = oneStopResults.getString(7);
          String flight_dest2 = oneStopResults.getString(10);
          int time1 = oneStopResults.getInt(11);
          int time2 = oneStopResults.getInt(12);
          int price1 = oneStopResults.getInt(15);
          int price2 = oneStopResults.getInt(16);
          int day = dayOfMonth;
          Flight f1 = getFlight(fid1);
          Flight f2 = getFlight(fid2);
          List<Flight> flights = new ArrayList<Flight>();
          flights.add(f1);
          flights.add(f2);
          Routes r = new Routes(0, time1 + time2, day, price1 + price2, flights, flight_origin1, flight_dest2);
          routes.add(r);
        }
        oneStopResults.close();
      }
      // merge the results and print them
    } catch (SQLException e) {
      e.printStackTrace();
      return "Failed to search\n";
    } finally {
      checkDanglingTransaction();
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

  /* See QueryAbstract.java for javadoc */
  public String transaction_book(int itineraryId) {
    // TODO: YOUR CODE HERE
    if (!isLoggedIn) {
      return "Cannot book reservations, not logged in\n";
    }
    // itinerary must be in the list of routes (which stores the most recent search performed within the same login session)
    if (routes == null || itineraryId < 0 || itineraryId >= routes.size()) {
      return "No such itinerary " + itineraryId + "\n";
    }
    try {
      boolean lock = true;
      while (lock){
        try {
          conn.setAutoCommit(false);
          Routes r = routes.get(itineraryId);
          // check if the user has a reservation on the same day as the one they are trying to book now
          check_same_day_reservation_stmt.clearParameters();
          check_same_day_reservation_stmt.setString(1, this.username);
          check_same_day_reservation_stmt.setInt(2, r.date);
          ResultSet rs = check_same_day_reservation_stmt.executeQuery();
          while (rs.next()) {
            rs.close();
            conn.rollback();
            return "You cannot book two flights in the same day\n";
          }
          // check if the flight(s) maximum capacity would be exceeded (it needs >= 1 seat available)
          for (Flight f : r.flights) {
            get_num_reserved_stmt.clearParameters();
            get_num_reserved_stmt.setInt(1, f.fid);
            get_num_reserved_stmt.setInt(2, f.fid);
            ResultSet num_reserved = get_num_reserved_stmt.executeQuery();
            num_reserved.next();
            int capacity = f.capacity - num_reserved.getInt(1);
            if (capacity <= 0) {
              conn.rollback();
              return "Booking failed\n";
            }
          }
          // get current reservation ID maximum
          ResultSet prev_RID = get_curr_rid_max_stmt.executeQuery();
          prev_RID.next();
          int rid = prev_RID.getInt(1) + 1;
          prev_RID.close();

          // create the reservation
          book_reservation_stmt.clearParameters();
          book_reservation_stmt.setInt(1, rid);
          book_reservation_stmt.setString(2, this.username);
          book_reservation_stmt.setInt(3, r.date);
          book_reservation_stmt.setInt(4, 0);
          book_reservation_stmt.setInt(5, r.price);
          book_reservation_stmt.setInt(6, r.flights.get(0).fid);
          // if we have a direct flight, set the second flight to null and the direct boolean to true
          if (r.flights.size() == 1) {
            book_reservation_stmt.setNull(7, java.sql.Types.INTEGER);
            book_reservation_stmt.setInt(8, 0);
          } else {
            book_reservation_stmt.setInt(7, r.flights.get(1).fid);
            book_reservation_stmt.setInt(8, 1);
          }
          book_reservation_stmt.executeUpdate();
          conn.commit();
          return "Booked flight(s), reservation ID: " + rid + "\n";          
        } catch (SQLException e) {
          lock = isDeadlock(e);
          conn.rollback();
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      checkDanglingTransaction();
    }
    return "Booking failed\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_pay(int reservationId) {
    // TODO: YOUR CODE HERE
    if (!isLoggedIn) {
      return "Cannot pay, not logged in\n";
    }
    boolean lock = true;
    try {
      while (lock){
        try {
          conn.setAutoCommit(false);
          check_reservation_status_stmt.clearParameters();
          check_reservation_status_stmt.setInt(1, reservationId);
          check_reservation_status_stmt.setString(2, this.username);
          ResultSet rs = check_reservation_status_stmt.executeQuery();
          rs.next();
          if (rs.getInt(1) == 0) {
            rs.close();
            conn.rollback();
            return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";
          }
          rs.close();
          // get the reservation
          get_reservation_stmt.clearParameters();
          get_reservation_stmt.setInt(1, reservationId);
          get_reservation_stmt.setString(2, this.username);
          ResultSet res = get_reservation_stmt.executeQuery();
          res.next();
          int price = res.getInt("price");
          res.close(); 
          // check if the user has enough money in their account
          check_user_exists_stmt.clearParameters();
          check_user_exists_stmt.setString(1, this.username);
          ResultSet user_info = check_user_exists_stmt.executeQuery();
          user_info.next();
          int balance = user_info.getInt("balance");
          user_info.close();
          if (balance < price) {
            conn.rollback();
            return "User has only " + balance + " in account but itinerary costs " + price + "\n";
          }
          // update the reservation to be paid
          update_paid_stmt.clearParameters();
          update_paid_stmt.setInt(1, reservationId);
          update_paid_stmt.setString(2, this.username);
          update_paid_stmt.executeUpdate();
          // update the user's balance
          int new_balance = balance - price;
          update_user_balance_stmt.clearParameters();
          update_user_balance_stmt.setInt(1, new_balance);
          update_user_balance_stmt.setString(2, this.username);
          update_user_balance_stmt.executeUpdate();
          conn.commit();
          return "Paid reservation: " + reservationId + " remaining balance: " + new_balance + "\n";
          } catch (SQLException e){
            lock = isDeadlock(e);
            conn.rollback();
          }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      checkDanglingTransaction();
    }
    return "Failed to pay for reservation " + reservationId + "\n";
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_reservations() {
    // TODO: YOUR CODE HERE
    if (!isLoggedIn) {
      return "Cannot view reservations, not logged in\n";
    }
    try {
      get_user_reservation_stmt.clearParameters();
      get_user_reservation_stmt.setString(1, this.username);
      ResultSet rs = get_user_reservation_stmt.executeQuery();
      StringBuilder str = new StringBuilder();
      while (rs.next()){
        int paid = rs.getInt("paid");
        int rid = rs.getInt("rid");
        int fid1 = rs.getInt("fid1");
        int fid2 = rs.getInt("fid2");
        if (paid == 1){
          str.append("Reservation " + rid + " paid: true:\n");
        } else {
          str.append("Reservation " + rid + " paid: false:\n");
        }
        List<Flight> flights = new ArrayList<>();
        Flight f1 = getFlight(fid1);
        flights.add(f1);
        // check if this is not a one hop flight
        if (fid2 != 0){
          Flight f2 = getFlight(fid2);
          flights.add(f2);
        }
        for (Flight f : flights){
          str.append(f);
          str.append("\n");
        }
      }
      rs.close();
      if (str.length() == 0) {
        return "No reservations found\n";
      }
      return str.toString();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      checkDanglingTransaction();
    }
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

  private Flight getFlight(int fid) throws SQLException{
    get_flight_stmt.clearParameters();
    get_flight_stmt.setInt(1, fid);
    ResultSet rs = get_flight_stmt.executeQuery();
    rs.next();
    int id = rs.getInt("fid");
    int day = rs.getInt("day_of_month");
    String carrierId = rs.getString("carrier_id");
    String flightNum = rs.getString("flight_num");
    String flight_origin = rs.getString("origin_city");
    String flight_dest = rs.getString("dest_city");
    int time = rs.getInt("actual_time");
    int capacity = rs.getInt("capacity");
    int price = rs.getInt("price");
    Flight f = new Flight(id, day, carrierId, flightNum, flight_origin, flight_dest, time, capacity, price);
    rs.close();
    return f;
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
