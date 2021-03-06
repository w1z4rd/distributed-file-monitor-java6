package org.costa;

import static org.costa.DFMProperties.getProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.costa.FileEntry.FileEntryBuilder;

public class DatabaseUtil {
	
	private final static String DATABASE_SERVER_ADDRESS = getProperty("databaseServerAddress");
	private final static String DATABASE_SERVER_PORT = getProperty("databaseServerPort");
	private final static String DATABASE_NAME = getProperty("databaseName");
	private final static String DATABASE_USERNAME = getProperty("databaseUserName");
	private final static String DATABASE_PASSWORD = getProperty("databasePassword");

	private static DatabaseUtil instance = new DatabaseUtil();
	private Connection connection = null;

	private DatabaseUtil() {
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://" + DATABASE_SERVER_ADDRESS + 
					":" + DATABASE_SERVER_PORT + "/" + DATABASE_NAME, DATABASE_USERNAME,
					DATABASE_PASSWORD);
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
				result = createFileEntity(resultSet);
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

	public boolean updateStatus(FileEntry oldFile, FileEntry newFile) {
		System.out.println(Thread.currentThread().getName() + " | DatabaseUtil - updateStatusToProcessing - " + oldFile);
		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(
					"update file_queue set status = ?, last_modification_date  = ?, last_modified_by = ? "
							+ "where id = ? and last_modification_date = ? and last_modified_by = ? and status = ?");
			statement.setString(1, newFile.getStatus().name());
			statement.setTimestamp(2, newFile.getLastModifiedOn());
			statement.setString(3, newFile.getLastModifiedBy());
			statement.setLong(4, oldFile.getId());
			statement.setTimestamp(5, oldFile.getLastModifiedOn());
			statement.setString(6, oldFile.getLastModifiedBy());
			statement.setString(7, oldFile.getStatus().name());
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
				FileEntry file = createFileEntity(resultSet);
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
			statement.setString(2, file.getName().toLowerCase(Locale.getDefault()));
			statement.setTimestamp(3, file.getFileLastModifiedOn());
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				result = createFileEntity(resultSet);
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
	
	private FileEntry createFileEntity(ResultSet resultSet) throws SQLException {
		return new FileEntryBuilder()
						.withId(resultSet.getLong(1))
						.withName(resultSet.getString(2))
						.withStatus(FileEntryStatus.getByName(resultSet.getString(3)))
						.withFileLastModifiedOn(resultSet.getTimestamp(4))
						.withChecksum(resultSet.getLong(5))
						.withLastModifiedOn(resultSet.getTimestamp(6))
						.withLastModifiedBy(resultSet.getString(7))
						.withCreatedOn(resultSet.getTimestamp(8))
						.withCreatedBy(resultSet.getString(9))
						.build();
	}
}
