package project1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class Connections {

	public static int setUpDriver(){
		int connect = 0;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		    System.out.println("JDBC set up successfull");
		    connect=1; //
		}catch (ClassNotFoundException e) {
			connect=0;
		}
		return connect;
		
	}
	
	public static Connection connectDB(String url, String user, String pass){
		Connection conn = null;
		
		try {
			conn = DriverManager.getConnection(url, user, pass);
		
		} catch (SQLException e) {
			// failed to establish connection
			 System.out.println("SQLException: " + e.getMessage());
			 System.out.println("SQLState: " + e.getSQLState());
		}
		
		return conn;
	}
	
	public static void disConnectDB(Connection conn){
		if(null!=conn){
			try {
				conn.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Disconnect failed.");
			}
		}
	}
}
