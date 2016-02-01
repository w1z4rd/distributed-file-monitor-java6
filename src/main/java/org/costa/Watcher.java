package org.costa;

import static org.costa.FileEntryStatus.DONE;
import static org.costa.FileEntryStatus.MISSING;
import static org.costa.FileEntryStatus.PENDING;
import static org.costa.FileEntryStatus.PROCESSING;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
	private static FileSystemManager fsManager;
	private static String hostname;
	private static final DatabaseUtil DB_UTIL = DatabaseUtil.getInstance();

	private Watcher() {

	}

	public static void main(String[] args) throws IOException {
		System.out.println("===============Application Started!===============");
		System.out.println("Redirecting System.out and System.err to target/watcher.log");

		FileOutputStream log = new FileOutputStream("target/watcher.log");
		PrintStream printStream = new PrintStream(log);
		System.setOut(printStream);
		System.setErr(printStream);
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
		fsManager = VFS.getManager();
		FileObject watchedFolder = fsManager.resolveFile("file:///media/upload_test");
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
		fm.setDelay(5000);
		fm.addFile(watchedFolder);
		fm.start();
		System.out.println("===============Monitor Started!===============");
		Thread worker1 = new Thread(new FileProcessor());
		worker1.setName(hostname + "-1");
		worker1.setDaemon(true);
		Thread worker2 = new Thread(new FileProcessor());
		worker2.setName(hostname + "-2");
		worker2.setDaemon(true);
		Thread worker3 = new Thread(new FileProcessor());
		worker3.setName(hostname + "-3");
		worker3.setDaemon(true);
		worker1.start();
		System.out.println("===============Worker: " + worker1.getName() + " Started!===============");
		worker2.start();
		System.out.println("===============Worker: " + worker2.getName() + " Started!===============");
		worker3.start();
		System.out.println("===============Worker: " + worker3.getName() + " Started!===============");
		while (true) {

		}
	}

	private static boolean isFile(FileObject file) {
		try {
			return file.getType().equals(FileType.FILE);
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

	private static void onDelete(FileObject file) {
		try {
			System.out.println(
					Thread.currentThread().getName() + " | Watcher -  onDeleted - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException e) {
			e.printStackTrace();
		}
	}

	private static void onCreated(FileObject file) {
		System.out.println(
				Thread.currentThread().getName() + " | Watcher -  onCreated - " + file.getName().getBaseName());
		if (!isFile(file)) {
			System.out.println(Thread.currentThread().getName() + " | Watcher -  onCreated "
					+ file.getName().getBaseName() + " is a folder and is skiped");
			return;
		}
		FileEntry fileEntry = createNewFileEntry(file);
		FileEntry persistedFileEntry = DB_UTIL.findFile(fileEntry);
		if (persistedFileEntry != null && persistedFileEntry.getStatus() == MISSING) {
			updateStatusToPending(persistedFileEntry);
			return;
		}
		DB_UTIL.create(fileEntry);
	}

	private static FileEntry createNewFileEntry(FileObject file) {
		FileEntry fileEntry = null;
		try {
			long checksum = FileUtils.checksumCRC32(new File(file.getName().getPath()));
			Timestamp fileLastModifiedOn = new Timestamp(file.getContent().getLastModifiedTime());
			Timestamp now = new Timestamp(System.currentTimeMillis());
			fileEntry = new FileEntryBuilder(-1L, file.getName().getBaseName(), FileEntryStatus.PENDING,
					fileLastModifiedOn, checksum).withCreatedBy(hostname).withCreatedOn(now)
							.withLastModifiedBy(hostname).withLastModifiedOn(now).build();
		} catch (FileSystemException fse) {
			fse.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				file.getContent().close();
				file.close();
			} catch (FileSystemException e) {
				e.printStackTrace();
			}
		}
		return fileEntry;
	}

	private static void onChanged(FileObject file) {
		try {
			System.out.println(Thread.currentThread().getName() + " | changed - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException fse) {
			fse.printStackTrace();
		}
	}

	private static void updateStatusToPending(FileEntry file) {
		System.out
				.println(Thread.currentThread().getName() + " | Watcher - updateStatusToPending - " + file);
		FileEntry reloadedFile = DB_UTIL.getById(file.getId());

		if (!reloadedFile.equals(file)) {
			System.out.println(Thread.currentThread().getName()
					+ " | Watcher - updateStatusToPending - reloaded file is not equal to processing file");
			return;
		}

		if (!DB_UTIL.updateStatus(file, PENDING)) {
			System.out.println(Thread.currentThread().getName()
					+ " | Watcher - updateStatusToPending - failed to update status to PENDING");
			return;
		}

		System.out.println(Thread.currentThread().getName()
				+ " | Watcher - updateStatusToPending - updated status to PENDING");

	}

	private static class FileProcessor implements Runnable {

		@Override
		public void run() {
			while (true) {
				List<FileEntry> files = DB_UTIL.getFilesToBeProcessed();
				Collections.shuffle(files);
				for (FileEntry file : files) {
					process(file);
				}
				try {
					Thread.sleep(10000);
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}
		}

		private void process(FileEntry file) {
			System.out.println(Thread.currentThread().getName() + " | FileProcessor - process - processing " + file);
			FileEntry reloadedFile = DB_UTIL.getById(file.getId());
			if (!reloadedFile.equals(file)) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - reloaded file is not equal to processing file");
				return;
			}
			if (!DB_UTIL.updateStatus(file, PROCESSING)) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - failed to update status to processing");
				return;
			}
			System.out.println(Thread.currentThread().getName()
					+ " | FileProcessor - process - updated file status to PROCESSING");
			try {
				File archiveFile = new File("/media/archive/" + file.getName());
				File processingFile = new File("/media/upload_test/" + file.getName());
				FileUtils.moveFile(processingFile, archiveFile);
				DB_UTIL.updateStatus(reloadedFile, DONE);
				System.out.println(Thread.currentThread().getName() + " | FileProcessor - processed - " + file);
			} catch (FileNotFoundException fnfe) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - processing file not found marking it as MISSING!");
				DB_UTIL.updateStatus(reloadedFile, MISSING);
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
