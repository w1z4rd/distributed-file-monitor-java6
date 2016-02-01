package org.costa;

import static org.costa.FileEntryStatus.DONE;
import static org.costa.FileEntryStatus.MISSING;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
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
		for (FileObject file : existingFiles) {
			if (!isAlreadyQueued(file)) {
				onCreated(file);
			}
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
		System.out.println("===============Worker1 Started!===============");
		worker2.start();
		System.out.println("===============Worker2 Started!===============");
		worker3.start();
		System.out.println("===============Worker3 Started!===============");
		while (true) {

		}
	}

	private static boolean isAlreadyQueued(FileObject file) {
		FileEntry persistedFile = DB_UTIL.findByName(file.getName().getBaseName());
		if (persistedFile == null) {
			return false;
		}
		return !(persistedFile.getStatus().equals("DONE") || persistedFile.getStatus().equals("MISSING"));
	}

	private static void onDelete(FileObject file) {
		try {
			System.out.println(Thread.currentThread().getName() + " | deleted - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException e) {
			e.printStackTrace();
		}
	}

	private static void onCreated(FileObject file) {
		try {
			if (!file.getType().equals(FileType.FILE)) {
				System.out.println(Thread.currentThread().getName() + " | " + file.getName().getBaseName()
						+ " is a folder and is skiped");
				return;
			}
			System.out.println(Thread.currentThread().getName() + " | created - " + file.getName().getBaseName());
			long checksum = FileUtils.checksumCRC32(new File(file.getName().getPath()));
			Timestamp fileLastModifiedOn = new Timestamp(file.getContent().getLastModifiedTime());
			Timestamp now = new Timestamp(System.currentTimeMillis());
			FileEntry fileEntry = new FileEntryBuilder(-1L, file.getName().getBaseName(), FileEntryStatus.PENDING,
					fileLastModifiedOn, checksum).withCreatedBy(hostname).withCreatedOn(now)
							.withLastModifiedBy(hostname).withLastModifiedOn(now).build();
			DB_UTIL.create(fileEntry);
		} catch (FileSystemException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				file.getContent().close();
				file.close();
			} catch (FileSystemException e) {
				e.printStackTrace();
			}
		}
	}

	private static void onChanged(FileObject file) {
		try {
			System.out.println(Thread.currentThread().getName() + " | changed - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException e) {
			e.printStackTrace();
		}
	}

	static class FileProcessor implements Runnable {

		@Override
		public void run() {
			while (true) {
				List<FileEntry> files = DB_UTIL.getFilesToBeProcessed();
				Collections.shuffle(files);
				for (FileEntry file : files) {
					if (process(file)) {
						Timestamp now = new Timestamp(System.currentTimeMillis());
						FileEntry updatedFile = new FileEntryBuilder(file.getId(), file.getName(), DONE,
								file.getFileLastModifiedOn(), file.getChecksum()).withCreatedBy(file.getCreatedBy())
										.withCreatedOn(file.getCreatedOn())
										.withLastModifiedBy(Thread.currentThread().getName()).withLastModifiedOn(now)
										.build();
						DB_UTIL.update(updatedFile);
					}
				}
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		private boolean process(FileEntry file) {
			System.out.println(Thread.currentThread().getName() + " | FileProcessor - process - processing " + file);
			FileEntry reloadedFile = DB_UTIL.getById(file.getId());
			if (!reloadedFile.equals(file)) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - reloaded file is not equal to processing file");
				return false;
			}
			if (!DB_UTIL.updateStatusToProcessing(file)) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - failed to update status to processing");
				return false;
			}
			System.out.println(Thread.currentThread().getName()
					+ " | FileProcessor - process - updated file status to PROCESSING");
			try {
				File archiveFile = new File("/media/archive/" + file.getName());
				File processingFile = new File("/media/upload_test/" + file.getName());
				FileUtils.moveFile(processingFile, archiveFile);
			} catch (FileNotFoundException fnfe) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - processing file not found marking it as MISSING!");
				fnfe.printStackTrace();
				Timestamp now = new Timestamp(System.currentTimeMillis());
				FileEntry updatedFile = new FileEntryBuilder(file.getId(), file.getName(), MISSING,
						file.getFileLastModifiedOn(), file.getChecksum()).withCreatedBy(file.getCreatedBy())
								.withCreatedOn(file.getCreatedOn()).withLastModifiedBy(Thread.currentThread().getName())
								.withLastModifiedOn(now).build();
				DB_UTIL.update(updatedFile);
				return false;
			} catch (FileExistsException fee) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - processing file already exists in archive folder!");
			} catch (IOException ioe) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - failed to move the file to archive");
				ioe.printStackTrace();
				return false;
			}
			System.out.println(Thread.currentThread().getName() + " | FileProcessor - processed - " + file);
			return true;
		}

	}
}
