package org.costa;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.costa.FileEntry.FileEntryBuilder;

public class DatabaseUtil {

	private static DatabaseUtil instance = new DatabaseUtil();
	private Connection connection = null;

	private DatabaseUtil() {
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://192.168.1.228:5432/upload_test", "postgres",
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

	public FileEntry create(FileEntry file) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - create - " + file);
		PreparedStatement statement = null;
		PreparedStatement getIdStatement = null;
		ResultSet resultSet = null;
		FileEntry result = null;
		try {
			statement = connection.prepareStatement(
					"insert into file_queue (file_name, status, file_last_modification_date, file_checksum, last_modification_date, last_modified_by, creation_date, created_by) "
							+ "values (?, ?, ?, ?, ?, ?, ?, ?)");
			statement.setString(1, file.getName());
			statement.setString(2, file.getStatus().name());
			statement.setTimestamp(3, file.getFileLastModifiedOn());
			statement.setLong(4, file.getChecksum());
			statement.setTimestamp(5, file.getLastModifiedOn());
			statement.setString(6, file.getLastModifiedBy());
			statement.setTimestamp(7, file.getCreatedOn());
			statement.setString(8, file.getCreatedBy());
			statement.executeUpdate();
			getIdStatement = connection.prepareStatement(
					"select * from file_queue where file_name like ? and file_last_modification_date = ? and created_on = ? and created_by = ?");
			getIdStatement.setString(1, file.getName());
			getIdStatement.setTimestamp(2, file.getFileLastModifiedOn());
			getIdStatement.setTimestamp(3, file.getCreatedOn());
			getIdStatement.setString(4, file.getCreatedBy());
			getIdStatement.execute();
			resultSet = getIdStatement.getResultSet();
			if (resultSet.next()) {
				result = new FileEntryBuilder(resultSet.getLong(1), resultSet.getString(2), 
						FileEntryStatus.getByName(resultSet.getString(3)),
						resultSet.getTimestamp(4), resultSet.getLong(5))
						.withLastModifiedOn(resultSet.getTimestamp(6))
						.withLastModifiedBy(resultSet.getString(7))
						.withCreatedOn(resultSet.getTimestamp(8))
						.withCreatedBy(resultSet.getString(9)).build();
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
		return result;
	}

	public FileEntry getById(long id) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - getById - " + id);
		PreparedStatement statement = null;
		FileEntry result = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement("select * from file_queue where id = ?");
			statement.setLong(1, id);
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				result = new FileEntryBuilder(resultSet.getLong(1), resultSet.getString(2), 
						FileEntryStatus.getByName(resultSet.getString(3)),
						resultSet.getTimestamp(4), resultSet.getLong(5))
						.withLastModifiedOn(resultSet.getTimestamp(6))
						.withLastModifiedBy(resultSet.getString(7))
						.withCreatedOn(resultSet.getTimestamp(8))
						.withCreatedBy(resultSet.getString(9)).build();
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
			statement.setTimestamp(2, file.getLastModifiedOn());
			statement.setString(3, file.getLastModifiedBy());
			statement.setLong(4, file.getId());
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
			statement.setLong(4, file.getId());
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
					"select * from file_queue where status like ? or (status like ? and last_modification_date < ?)");
			statement.setString(1, FileEntryStatus.PENDING.name());
			statement.setString(2, FileEntryStatus.PROCESSING.name());
			statement.setTimestamp(3, timestamp);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				FileEntry file = new FileEntryBuilder(resultSet.getLong(1), resultSet.getString(2), 
						FileEntryStatus.getByName(resultSet.getString(3)),
						resultSet.getTimestamp(4), resultSet.getLong(5))
						.withLastModifiedOn(resultSet.getTimestamp(6))
						.withLastModifiedBy(resultSet.getString(7))
						.withCreatedOn(resultSet.getTimestamp(8))
						.withCreatedBy(resultSet.getString(9)).build();
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
				result = new FileEntryBuilder(resultSet.getLong(1), resultSet.getString(2), 
						FileEntryStatus.getByName(resultSet.getString(3)),
						resultSet.getTimestamp(4), resultSet.getLong(5))
						.withLastModifiedOn(resultSet.getTimestamp(6))
						.withLastModifiedBy(resultSet.getString(7))
						.withCreatedOn(resultSet.getTimestamp(8))
						.withCreatedBy(resultSet.getString(9)).build();
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
	
	public FileEntry findFile(FileEntry file) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - findFile - " + file);
		PreparedStatement statement = null;
		FileEntry result = null;
		ResultSet resultSet = null;
		try {
			statement = connection.prepareStatement("select * from file_queue where file_checksum = ? and file_last_modification_date = ? and lower(file_name) = ?");
			statement.setLong(1, file.getChecksum());
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				result = new FileEntryBuilder(resultSet.getLong(1), resultSet.getString(2), 
						FileEntryStatus.getByName(resultSet.getString(3)),
						resultSet.getTimestamp(4), resultSet.getLong(5))
						.withLastModifiedOn(resultSet.getTimestamp(6))
						.withLastModifiedBy(resultSet.getString(7))
						.withCreatedOn(resultSet.getTimestamp(8))
						.withCreatedBy(resultSet.getString(9)).build();
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
