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

	private static DatabaseUtil instance = new DatabaseUtil();
	private Connection connection = null;

	private DatabaseUtil() {
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://192.168.6.28:5432/upload_test", "postgres",
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
					"insert into file_queue (file_name, status, file_last_modification_date, last_modification_date, last_modified_by, creation_date, created_by) "
							+ "values (?, ?, ?, ?, ?, ?, ?)");
			statement.setString(1, file.getName());
			statement.setString(2, file.getStatus().name());
			statement.setTimestamp(3, file.getFileLastModifiedOn());
			statement.setTimestamp(4, file.getLastModifiedOn());
			statement.setString(5, file.getLastModifiedBy());
			statement.setTimestamp(6, file.getCreatedOn());
			statement.setString(7, file.getCreatedBy());
			statement.executeUpdate();
			getIdStatement = connection.prepareStatement(
					"select id from file_queue where file_name like ? and file_last_modification_date = ? and created_on = ? and created_by = ?");
			getIdStatement.setString(1, file.getName());
			getIdStatement.setTimestamp(2, file.getFileLastModifiedOn());
			getIdStatement.setTimestamp(3, file.getCreatedOn());
			getIdStatement.setString(4, file.getCreatedBy());
			getIdStatement.execute();
			resultSet = getIdStatement.getResultSet();
			if (resultSet.next()) {
				file.setId(resultSet.getInt(1));
			}
		} catch (SQLException e) {
			System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - create - failed to create" + file
					+ "\n" + e.getMessage());
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
			if (getIdStatement != null) {
				try {
					getIdStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
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
				result.setStatus(FileEntryStatus.getByName(resultSet.getString(3)));
				result.setFileLastModifiedOn(resultSet.getTimestamp(4));
				result.setLastModifiedOn(resultSet.getTimestamp(5));
				result.setLastModifiedBy(resultSet.getString(6));
				result.setCreatedOn(resultSet.getTimestamp(7));
				result.setCreatedBy(resultSet.getString(8));
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
					"update file_queue set status = ?, last_modification_date = ?, last_modified_by = ? where id = ?");
			statement.setString(1, file.getStatus().name());
			statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			statement.setString(3, file.getLastModifiedBy());
			statement.setInt(4, file.getId());
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
			statement = connection.prepareStatement(
					"update file_queue set status = ?, last_modification_date  = ?, last_modified_by = ? "
							+ "where id = ? and last_modification_date = ? and last_modified_by = ? and status = ?");
			statement.setString(1, FileEntryStatus.PROCESSING.name());
			statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			statement.setString(3, Thread.currentThread().getName());
			statement.setInt(4, file.getId());
			statement.setTimestamp(5, file.getLastModifiedOn());
			statement.setString(6, file.getLastModifiedBy());
			statement.setString(7, file.getStatus().name());
			return statement.executeUpdate() == 1;
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
		List<FileEntry> result = new ArrayList<FileEntry>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		Timestamp timestamp = new Timestamp(System.currentTimeMillis() - 1000 * 60 * 30);
		try {
			statement = connection.prepareStatement(
					"select * from file_queue where status like 'PENDING' or (status like 'PROCESSING' and last_modification_date < ?)");
			statement.setTimestamp(1, timestamp);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				FileEntry file = new FileEntry();
				file.setId(resultSet.getInt(1));
				file.setName(resultSet.getString(2));
				file.setStatus(FileEntryStatus.getByName(resultSet.getString(3)));
				file.setFileLastModifiedOn(resultSet.getTimestamp(4));
				file.setLastModifiedOn(resultSet.getTimestamp(5));
				file.setLastModifiedBy(resultSet.getString(6));
				file.setCreatedOn(resultSet.getTimestamp(7));
				file.setCreatedBy(resultSet.getString(8));
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
		return result;
	}

	public FileEntry findByName(String name) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - findByName - " + name);
		PreparedStatement statement = null;
		FileEntry result = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement("select * from file_queue where file_name = ?");
			statement.setString(1, name);
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				result = new FileEntry();
				result.setId(resultSet.getInt(1));
				result.setName(resultSet.getString(2));
				result.setStatus(FileEntryStatus.getByName(resultSet.getString(3)));
				result.setFileLastModifiedOn(resultSet.getTimestamp(4));
				result.setLastModifiedOn(resultSet.getTimestamp(5));
				result.setLastModifiedBy(resultSet.getString(6));
				result.setCreatedOn(resultSet.getTimestamp(7));
				result.setCreatedBy(resultSet.getString(8));
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
}
