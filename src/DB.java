package rs.etf.sab.student;

import java.sql.*;

public class DB {

	private static final String username = "sa";
	private static final String password = "123";
	private static final String database = "vl190384";
	private static final int port = 1433;
	private static final String server = "localhost";

	private static final String connectionUrl = "jdbc:sqlserver://" + server + ":" + port + ";databaseName=" + database
			+ ";encrypt=true" + ";trustServerCertificate=true";

	private Connection connection;

	public Connection getConnection() {
		return connection;
	}

	private DB() {
		try {
			connection = DriverManager.getConnection(connectionUrl, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static DB db = null;

	public static DB getInstance() {
		if (db == null) {
			db = new DB();
		}
		return db;
	}
}
