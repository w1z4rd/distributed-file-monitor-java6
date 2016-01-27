package org.costa;

import java.io.IOException;
import java.util.List;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;

public class Watcher {
	private static FileSystemManager fsManager;
	private static final DatabaseUtil dbUtil = DatabaseUtil.getInstance();

	public static void main(String[] args) throws IOException {
		System.out.println("===============Application Started!===============");
		fsManager = VFS.getManager();
		FileObject watchedFolder = fsManager.resolveFile("file:///media/upload_test");
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
		fm.addFile(watchedFolder);
		fm.start();
		System.out.println("===============Monitor Started!===============");
		Thread worker1 = new Thread(new FileProcessor());
		worker1.setName("bunti1-1");
		Thread worker2 = new Thread(new FileProcessor());
		worker2.setName("bunti1-2");
		Thread worker3 = new Thread(new FileProcessor());
		worker3.setName("bunti1-3");
		worker1.start();
		System.out.println("===============Worker1 Started!===============");
		worker2.start();
		System.out.println("===============Worker2 Started!===============");
		worker3.start();
		System.out.println("===============Worker3 Started!===============");
		while (true) {

		}
	}

	private static void onDelete(FileObject file) {
		try {
			System.out.println("deleted - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException e) {
			e.printStackTrace();
		}
	}

	private static void onCreated(FileObject file) {
		try {
			if (!file.getType().equals(FileType.FILE)) {
				System.out.println(file.getName().getBaseName() + " is a folder and is skiped");
				file.close();
				return;
			}
			System.out.println("created - " + file.getName().getBaseName());
			FileEntry fileEntry = new FileEntry(file.getName().getBaseName());
			dbUtil.create(fileEntry);
			file.close();
		} catch (FileSystemException e) {
			e.printStackTrace();
		}
	}

	private static void onChanged(FileObject file) {
		try {
			System.out.println("changed - " + file.getName().getBaseName());
			file.close();
		} catch (FileSystemException e) {
			e.printStackTrace();
		}
	}

	static class FileProcessor implements Runnable {

		@Override
		public void run() {
			while (true) {
				List<FileEntry> files = dbUtil.getFilesToBeProcessed();
				for (FileEntry file : files) {
					if (process(file)) {
						file.setStatus("DONE");
						file.setUpdatedBy(Thread.currentThread().getName());
						dbUtil.update(file);
					}
				}
			}
		}

		private boolean process(FileEntry file) {
			System.out.println(Thread.currentThread().getName() + " | FileProcessor - process - " + file);
			FileEntry reloadedFile = dbUtil.getById(file.getId());
			if (!reloadedFile.equals(file)) {
				System.out.println(Thread.currentThread().getName()
						+ " | FileProcessor - process - reloadedFile is not equal to processing file");
				return false;
			}
			if (!dbUtil.updateStatusToProcessing(file)) {
				System.out.println(Thread.currentThread().getName() + "failed to update status to processing");
				return false;
			}
			FileObject archiveFile = null;
			FileObject processingFile = null;
			try {
				archiveFile = fsManager.resolveFile("file:///media/archive/" + file.getName());
				processingFile = fsManager.resolveFile("file:///media/upload_test/" + file.getName());
				processingFile.moveTo(archiveFile);
				archiveFile.close();
				processingFile.close();
			} catch (FileSystemException e) {
				e.printStackTrace();
				return false;
			} finally {
				if (archiveFile != null) {
					try {
						archiveFile.close();
					} catch (FileSystemException e) {
						e.printStackTrace();
						return false;
					}
				}
				if (processingFile != null) {
					try {
						processingFile.close();
					} catch (FileSystemException e) {
						e.printStackTrace();
						return false;
					}
				}
			}
			return true;
		}

	}
}
