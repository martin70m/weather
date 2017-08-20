package de.martin70m.common.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MySqlConnection {
	
	private static final String serverName = "mysql6.1blu.de";
	private static final String portNumber = "3306"; 
	private static final String dataBase = "db1414x685817";
	private static final String dbms = "mysql";
	private static final String userName = "sql1414_685817";
	private static String password = "";

	
	public MySqlConnection(String password) {
		if(password.equals("")) 
			MySqlConnection.password = password;
	}
	public Connection getConnection() throws SQLException {

	    Connection conn = null;
	    Properties connectionProps = new Properties();
	    connectionProps.put("user", MySqlConnection.userName);
	    connectionProps.put("password", MySqlConnection.password);

	    conn = DriverManager.getConnection(
	                   "jdbc:" + MySqlConnection.dbms + "://" +
	                	MySqlConnection.serverName +
	                   ":" + MySqlConnection.portNumber + "/" + MySqlConnection.dataBase,
	                   connectionProps);
	    
	    System.out.println("Connected to database");
	    return conn;
	}
}
