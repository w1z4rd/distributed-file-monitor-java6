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
			connection = DriverManager.getConnection("jdbc:postgresql://192.168.6.129:5432/upload_test", "postgres",
					"postgres");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
	}

	public static DatabaseUtil getInstance() {
		return instance;
	}

	public void create(FileEntry file) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - create - " + file);
		PreparedStatement statement = null;
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
		} catch (SQLException e) {
			System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - create - failed to create" + file
					+ "\n" + e.getMessage());
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
		}
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
						FileEntryStatus.getByName(resultSet.getString(3)), resultSet.getTimestamp(4),
						resultSet.getLong(5)).withLastModifiedOn(resultSet.getTimestamp(6))
								.withLastModifiedBy(resultSet.getString(7)).withCreatedOn(resultSet.getTimestamp(8))
								.withCreatedBy(resultSet.getString(9)).build();
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
		}
		return result;
	}

	public boolean updateStatus(FileEntry file, FileEntryStatus status) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - updateStatusToProcessing - " + file);
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
					"update file_queue set status = ?, last_modification_date  = ?, last_modified_by = ? "
							+ "where id = ? and last_modification_date = ? and last_modified_by = ? and status = ?");
			statement.setString(1, status.name());
			statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			statement.setString(3, Thread.currentThread().getName());
			statement.setLong(4, file.getId());
			statement.setTimestamp(5, file.getLastModifiedOn());
			statement.setString(6, file.getLastModifiedBy());
			statement.setString(7, file.getStatus().name());
			return statement.executeUpdate() == 1;
		} catch (SQLException sqle) {
			sqle.printStackTrace();
			return false;
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
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
					"select * from file_queue where status != ? and status != ? and not (status = ? and last_modification_date > ?)");
			statement.setString(1, FileEntryStatus.DONE.name());
			statement.setString(2, FileEntryStatus.MISSING.name());
			statement.setString(3, FileEntryStatus.PROCESSING.name());
			statement.setTimestamp(4, timestamp);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				FileEntry file = new FileEntryBuilder(resultSet.getLong(1), resultSet.getString(2),
						FileEntryStatus.getByName(resultSet.getString(3)), resultSet.getTimestamp(4),
						resultSet.getLong(5)).withLastModifiedOn(resultSet.getTimestamp(6))
								.withLastModifiedBy(resultSet.getString(7)).withCreatedOn(resultSet.getTimestamp(8))
								.withCreatedBy(resultSet.getString(9)).build();
				result.add(file);
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
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
			statement = connection.prepareStatement(
					"select * from file_queue where file_checksum = ? and lower(file_name) = ? and file_last_modification_date = ?");
			statement.setLong(1, file.getChecksum());
			statement.setString(2, file.getName().toLowerCase());
			statement.setTimestamp(3, file.getFileLastModifiedOn());
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				result = new FileEntryBuilder(resultSet.getLong(1), resultSet.getString(2),
						FileEntryStatus.getByName(resultSet.getString(3)), resultSet.getTimestamp(4),
						resultSet.getLong(5)).withLastModifiedOn(resultSet.getTimestamp(6))
								.withLastModifiedBy(resultSet.getString(7)).withCreatedOn(resultSet.getTimestamp(8))
								.withCreatedBy(resultSet.getString(9)).build();
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException sqle) {
					sqle.printStackTrace();
				}
			}
		}
		return result;
	}
}
