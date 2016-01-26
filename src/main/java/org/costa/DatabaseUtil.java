package org.costa;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class DatabaseUtil {

	private static Connection connection = null;
	private static DatabaseUtil instance = new DatabaseUtil();

	private DatabaseUtil() {
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/upload_test", "postgres",
					"postgres");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static DatabaseUtil getInstance() {
		return instance;
	}

	public void create(FileEntry file) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - create - " + file);
		PreparedStatement statement = null;
		PreparedStatement getIdStatement = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement(
					"insert into file_queue (file_name, status, version, updated_on, updated_by) values (?, ?, ?, ?, ?)");
			statement.setString(1, file.getName());
			statement.setString(2, file.getStatus());
			statement.setInt(3, file.getVersion());
			statement.setTimestamp(4, file.getUpdatedOn());
			statement.setString(5, file.getUpdatedBy());
			statement.executeUpdate();
			getIdStatement = connection
					.prepareStatement("select id from file_queue where file_name like ? and updated_on = ?");
			getIdStatement.setString(1, file.getName());
			getIdStatement.setTimestamp(2, file.getUpdatedOn());
			getIdStatement.execute();
			resultSet = getIdStatement.getResultSet();
			if (resultSet.next()) {
				file.setId(resultSet.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
					;
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (getIdStatement != null) {
				try {
					getIdStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
					;
				}
			}
		}
	}

	public FileEntry getById(int id) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - getById - " + id);
		PreparedStatement statement = null;
		FileEntry result = new FileEntry();
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement("select * from file_queue where id = ?");
			statement.setInt(1, id);
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				result.setId(resultSet.getInt(1));
				result.setName(resultSet.getString(2));
				result.setStatus(resultSet.getString(3));
				result.setVersion(resultSet.getInt(4));
				result.setUpdatedOn(resultSet.getTimestamp(5));
				result.setUpdatedBy(resultSet.getString(6));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	public boolean update(FileEntry file) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - update - " + file);
		PreparedStatement statement = null;
		boolean result = false;
		try {
			statement = connection.prepareStatement(
					"update file_queue set status = ?, version = ?, updated_on = ?, updated_by = ? where id = ?");
			statement.setString(1, file.getStatus());
			statement.setInt(2, file.getVersion());
			statement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			statement.setString(4, file.getUpdatedBy());
			statement.setInt(5, file.getId());
			result = statement.executeUpdate() == 1;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
					return false;
				}
			}
		}
		return result;
	}

	public boolean updateStatusToProcessing(FileEntry file) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - updateStatusToProcessing - " + file);
		PreparedStatement statement = null;
		try {
			statement = connection
					.prepareStatement("update file_queue set status = ?, version = ?, updated_on = ?, updated_by = ? "
							+ "where id = ? and updated_on = ? and version = ? and updated_by = ? and status = ?");
			statement.setString(1, "PROCESSING");
			statement.setInt(2, file.getVersion() + 1);
			statement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
			statement.setString(4, Thread.currentThread().getName());
			statement.setInt(5, file.getId());
			statement.setTimestamp(6, file.getUpdatedOn());
			statement.setInt(7, file.getVersion());
			statement.setString(8, file.getUpdatedBy());
			statement.setString(9, file.getStatus());
			boolean res = statement.executeUpdate() == 1;
			System.out
					.println(Thread.currentThread().getName() + " | DatabaseUtil - updateStatusToProcessing - " + res);
			return res;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
					return false;
				}
			}
		}
	}

	public List<FileEntry> getFilesToBeProcessed() {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - getFilesToBeProcessed");
		List<FileEntry> result = new ArrayList<FileEntry>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		Timestamp timestamp = new Timestamp(System.currentTimeMillis() - 1000 * 60 * 30);
		try {
			statement = connection.prepareStatement(
					"select * from file_queue where status like 'PENDING' or (status like 'PROCESSING' and updated_on < ?)");
			statement.setTimestamp(1, timestamp);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				FileEntry file = new FileEntry();
				file.setId(resultSet.getInt(1));
				file.setName(resultSet.getString(2));
				file.setStatus(resultSet.getString(3));
				file.setVersion(resultSet.getInt(4));
				file.setUpdatedOn(resultSet.getTimestamp(5));
				file.setUpdatedBy(resultSet.getString(6));
				result.add(file);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - getFilesToBeProcessed: " + result);
		return result;
	}
}
