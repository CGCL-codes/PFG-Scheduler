package com.sql.databaseconnection;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Connection {
static java.sql.Connection connection;
//public static void main(String[] args) throws SQLException {
//	Connection.connetion();
//	System.out.println("Connected");
//}
public static java.sql.Connection connetion() throws SQLException {
	// try {
	try {
		Class.forName("com.mysql.jdbc.Driver");
		//connection = (java.sql.Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/load_calculation", "root", "Root");
		
		connection = (java.sql.Connection) DriverManager.getConnection("jdbc:mysql://node82:3306/proposed_data_result", "username", "password");
		
		System.out.println("connected");
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	// System.out.println("asdeel");
	return (java.sql.Connection) connection;

}
}
