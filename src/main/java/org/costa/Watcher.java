package org.costa;

import static org.costa.DFMProperties.getLongProperty;
import static org.costa.DFMProperties.getProperty;
import static org.costa.FileEntryStatus.DONE;
import static org.costa.FileEntryStatus.MISSING;
import static org.costa.FileEntryStatus.PENDING;
import static org.costa.FileEntryStatus.PROCESSING;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;
import org.costa.FileEntry.FileEntryBuilder;

public class Watcher {
	private final static DatabaseUtil DB_UTIL = DatabaseUtil.getInstance();
	private final static String UPLOAD_FOLDER = getProperty("uploadFolder");
	private final static String ARCHIVE_FOLDER = getProperty("archvieFolder");
	private final static long WATCHER_SLEEP = getLongProperty("watcherSleepInMillis", 5000);
	private final static long WORKER_SLEEP = getLongProperty("workerSleepInMillis", 10000);
	private final static String INSTANCE_NAME = getProperty("instanceName");

	private static FileSystemManager fsManager;

	private Watcher() {

	}

	public static void main(String[] args) throws IOException {
		System.out.println("===============Application Started!===============");
		System.out.println("Redirecting System.out and System.err to target/watcher.log");

		FileOutputStream log = new FileOutputStream("target/watcher.log");
		PrintStream printStream = new PrintStream(log, true, "UTF-8");
		System.setOut(printStream);
		System.setErr(printStream);
		fsManager = VFS.getManager();
		FileObject watchedFolder = fsManager.resolveFile("file://" + UPLOAD_FOLDER);
		FileObject[] existingFiles = watchedFolder.getChildren();
		List<FileObject> existingFilesList = Arrays.asList(existingFiles);
		Collections.shuffle(existingFilesList);
		for (FileObject file : existingFilesList) {
			onCreated(file);
		}
		DefaultFileMonitor fm = new DefaultFileMonitor(new FileListener() {

			@Override
			public void fileDeleted(FileChangeEvent fileChangeEvent) throws Exception {
				onDelete(fileChangeEvent.getFile());
			}

			@Override
			public void fileCreated(FileChangeEvent fileChangeEvent) throws Exception {
				onCreated(fileChangeEvent.getFile());
			}

			@Override
			public void fileChanged(FileChangeEvent fileChangeEvent) throws Exception {
				onChanged(fileChangeEvent.getFile());
			}
		});
		fm.setRecursive(false);
		fm.setDelay(WATCHER_SLEEP);
		fm.addFile(watchedFolder);
		fm.start();
		System.out.println("===============Monitor Started!===============");
		Thread worker1 = new Thread(new FileProcessor());
		worker1.setName(INSTANCE_NAME + "-worker-1");
		worker1.setDaemon(true);
		Thread worker2 = new Thread(new FileProcessor());
		worker2.setName(INSTANCE_NAME + "-worker-2");
		worker2.setDaemon(true);
		Thread worker3 = new Thread(new FileProcessor());
		worker3.setName(INSTANCE_NAME + "-worker-3");
		worker3.setDaemon(true);
		worker1.start();
		System.out.println("===============" + worker1.getName() + " Started!===============");
		worker2.start();
		System.out.println("===============" + worker2.getName() + " Started!===============");
		worker3.start();
		System.out.println("===============" + worker3.getName() + " Started!===============");
		while (true) {

		}
	}

	private static boolean isFile(final FileObject file) {
		try {
			return !file.getType().equals(FileType.FOLDER);
		} catch (FileSystemException fse) {
			fse.printStackTrace();
			return false;
		} finally {
			try {
				file.getContent().close();
				file.close();
			} catch (FileSystemException fse) {
				fse.printStackTrace();
			}
		}
	}

	private static void onDelete(final FileObject file) {
		try {
			System.out.println(
					Thread.currentThread().getName() + " | Watcher -  onDeleted - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException e) {
			e.printStackTrace();
		}
	}

	private static void onCreated(final FileObject file) {
		System.out.println(
				Thread.currentThread().getName() + " | Watcher -  onCreated - " + file.getName().getBaseName());
		if (!isFile(file)) {
			System.out.println(Thread.currentThread().getName() + " | Watcher -  onCreated "
					+ file.getName().getBaseName() + " is a folder and is skiped");
			return;
		}
		FileEntry fileEntry = createNewFileEntry(file);
		if (fileEntry == null) {
			System.out.println("Failed to create fileEntry object!" + file);
			return;
		}
		FileEntry persistedFileEntry = DB_UTIL.findFile(fileEntry);
		if (persistedFileEntry != null) {
			switch (persistedFileEntry.getStatus()) {
			case MISSING:
				updateStatusToPending(persistedFileEntry);
				break;
			case DONE:
				System.out.println(Thread.currentThread().getName()
						+ " | Watcher -  onCreated - identical file already processed " + persistedFileEntry);
				break;
			default:
				break;
			}
			return;
		}
		DB_UTIL.create(fileEntry);
	}

	private static FileEntry createNewFileEntry(final FileObject file) {
		final FileEntryBuilder fileEntry = new FileEntryBuilder();
		final Timestamp now = new Timestamp(System.currentTimeMillis());
		fileEntry.withId(-1L)
			 .withName(file.getName().getBaseName())
			 .withStatus(PENDING)
			 .withCreatedBy(INSTANCE_NAME)
			 .withCreatedOn(now)
			 .withLastModifiedBy(INSTANCE_NAME)
			 .withLastModifiedOn(now);
		try {
			long checksum = FileUtils.checksumCRC32(new File(file.getName().getPath()));
			final Timestamp fileLastModifiedOn = new Timestamp(file.getContent().getLastModifiedTime());
			fileEntry.withFileLastModifiedOn(fileLastModifiedOn)
					.withChecksum(checksum);
		} catch (FileSystemException fse) {
			fse.printStackTrace();
			return null;
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return null;
		} finally {
			try {
				file.getContent().close();
				file.close();
			} catch (FileSystemException e) {
				e.printStackTrace();
			}
		}
		return fileEntry.build();
	}

	private static void onChanged(final FileObject file) {
		try {
			System.out.println(Thread.currentThread().getName() + " | changed - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException fse) {
			fse.printStackTrace();
		}
	}

	private static void updateStatusToPending(FileEntry file) {
		System.out.println(Thread.currentThread().getName() + " | Watcher - updateStatusToPending - " + file);
		FileEntry reloadedFile = DB_UTIL.getById(file.getId());

		if (!reloadedFile.equals(file)) {
			System.out.println(Thread.currentThread().getName()
					+ " | Watcher - updateStatusToPending - reloaded file is not equal to processing file");
			return;
		}
		FileEntry updatedFile = new FileEntryBuilder()
				.withId(file.getId())
				.withName(file.getName())
				.withStatus(PENDING)
				.withFileLastModifiedOn(file.getFileLastModifiedOn())
				.withChecksum(file.getChecksum())
				.withLastModifiedOn(new Timestamp(System.currentTimeMillis()))
				.withLastModifiedBy(Thread.currentThread().getName())
				.build();
		if (!DB_UTIL.updateStatus(file, updatedFile)) {
			System.out.println(Thread.currentThread().getName()
					+ " | Watcher - updateStatusToPending - failed to update status to PENDING");
			return;
		}

		System.out.println(
				Thread.currentThread().getName() + " | Watcher - updateStatusToPending - updated status to PENDING");

	}

	private static class FileProcessor implements Runnable {

		@Override
		public void run() {
			while (true) {
				final List<FileEntry> files = DB_UTIL.getFilesToBeProcessed();
				Collections.shuffle(files);
				for (final FileEntry file : files) {
					process(file);
				}
				try {
					Thread.sleep(WORKER_SLEEP);
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}
		}

		private void process(final FileEntry inputFile) {
			System.out
					.println(Thread.currentThread().getName() + " | FileProcessor - process - processing " + inputFile);
			final FileEntry reloadedFile = DB_UTIL.getById(inputFile.getId());
			if (!reloadedFile.equals(inputFile)) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - reloaded file is not equal to processing file");
				return;
			}
			final FileEntry processingFile = new FileEntryBuilder()
							.withId(inputFile.getId())
							.withStatus(PROCESSING)
							.withLastModifiedOn(new Timestamp(System.currentTimeMillis()))
							.withLastModifiedBy(Thread.currentThread().getName())
							.build();
			if (!DB_UTIL.updateStatus(inputFile, processingFile)) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - failed to update status to processing");
				return;
			}
			System.out.println(Thread.currentThread().getName()
					+ " | FileProcessor - process - updated file status to PROCESSING");
			try {
				File archivedFile = new File(ARCHIVE_FOLDER + "/" + inputFile.getName());
				File uploadedFile = new File(UPLOAD_FOLDER + "/" + inputFile.getName());
				FileUtils.moveFile(uploadedFile, archivedFile);
				final FileEntry doneFile = new FileEntryBuilder()
						.withId(inputFile.getId())
						.withStatus(DONE)
						.withLastModifiedOn(new Timestamp(System.currentTimeMillis()))
						.withLastModifiedBy(Thread.currentThread().getName())
						.build();
				DB_UTIL.updateStatus(processingFile, doneFile);
				System.out.println(Thread.currentThread().getName() + " | FileProcessor - processed - " + inputFile);
			} catch (FileNotFoundException fnfe) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - processing file not found marking it as MISSING!");
				final FileEntry missingFile = new FileEntryBuilder()
						.withId(inputFile.getId())
						.withStatus(MISSING)
						.withLastModifiedOn(new Timestamp(System.currentTimeMillis()))
						.withLastModifiedBy(Thread.currentThread().getName())
						.build();
				DB_UTIL.updateStatus(processingFile, missingFile);
				fnfe.printStackTrace();
			} catch (FileExistsException fee) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - processing file already exists in archive folder!");
				fee.printStackTrace();
			} catch (IOException ioe) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - failed to move the file to archive");
				ioe.printStackTrace();
			}
		}
	}
}
