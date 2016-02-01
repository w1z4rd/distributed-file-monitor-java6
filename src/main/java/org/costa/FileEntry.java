package org.costa;

import java.sql.Timestamp;

public class FileEntry {
	private long id;
	private long checksum;
	
	private String name;
	private String createdBy;
	private String lastModifiedBy;

	private Timestamp createdOn;
	private Timestamp lastModifiedOn;
	private Timestamp fileLastModifiedOn;

	private FileEntryStatus status;
	
	private FileEntry(FileEntryBuilder builder) {
		this.id = builder.id;
		this.name = builder.name;
		this.status = builder.status;
		this.lastModifiedBy = builder.lastModifiedBy;
		this.checksum = builder.checksum;
		this.createdBy = builder.createdBy;
		this.lastModifiedOn = builder.lastModifiedOn;
		this.createdOn = builder.createdOn;
		this.fileLastModifiedOn = builder.fileLastModifiedOn;
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public FileEntryStatus getStatus() {
		return status;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public String getLastModifiedBy() {
		return lastModifiedBy;
	}

	public long getChecksum() {
		return checksum;
	}

	public Timestamp getCreatedOn() {
		return createdOn;
	}

	public Timestamp getLastModifiedOn() {
		return lastModifiedOn;
	}

	public Timestamp getFileLastModifiedOn() {
		return fileLastModifiedOn;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 17;
		result = prime * result + Long.valueOf(id).hashCode();
		result = prime * result + Long.valueOf(checksum).hashCode();
		result = prime * result + ((createdBy == null) ? 0 : createdBy.hashCode());
		result = prime * result + ((createdOn == null) ? 0 : createdOn.hashCode());
		result = prime * result + ((fileLastModifiedOn == null) ? 0 : fileLastModifiedOn.hashCode());
		result = prime * result + ((lastModifiedBy == null) ? 0 : lastModifiedBy.hashCode());
		result = prime * result + ((lastModifiedOn == null) ? 0 : lastModifiedOn.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FileEntry other = (FileEntry) obj;
		if (status != other.status)
			return false;
		if (lastModifiedOn == null) {
			if (other.lastModifiedOn != null)
				return false;
		} else if (!lastModifiedOn.equals(other.lastModifiedOn))
			return false;
		if (checksum != other.checksum)
			return false;
		if (lastModifiedBy == null) {
			if (other.lastModifiedBy != null)
				return false;
		} else if (!lastModifiedBy.equals(other.lastModifiedBy))
			return false;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (createdBy == null) {
			if (other.createdBy != null)
				return false;
		} else if (!createdBy.equals(other.createdBy))
			return false;
		if (createdOn == null) {
			if (other.createdOn != null)
				return false;
		} else if (!createdOn.equals(other.createdOn))
			return false;
		if (fileLastModifiedOn == null) {
			if (other.fileLastModifiedOn != null)
				return false;
		} else if (!fileLastModifiedOn.equals(other.fileLastModifiedOn))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FileEntry [id=" + id + ", name=" + name + ", status=" + status + ", createdBy=" + createdBy
				+ ", lastModifiedBy=" + lastModifiedBy + ", checksum=" + checksum + ", createdOn=" + createdOn
				+ ", lastModifiedOn=" + lastModifiedOn + ", fileLastModifiedOn=" + fileLastModifiedOn + "]";
	}

	public static class FileEntryBuilder {
		private long id;
		private String name;
		private FileEntryStatus status;
		private String createdBy;
		private String lastModifiedBy;
		private long checksum;
		private Timestamp createdOn;
		private Timestamp lastModifiedOn;
		private Timestamp fileLastModifiedOn;

		public FileEntryBuilder(long id, String name, FileEntryStatus status, Timestamp fileLastModifiedOn,
				long checksum) {
			this.id = id;
			this.name = name;
			this.status = status;
			this.fileLastModifiedOn = fileLastModifiedOn;
			this.checksum = checksum;
		}

		public FileEntryBuilder withCreatedBy(String createdBy) {
			this.createdBy = createdBy;
			return this;
		}

		public FileEntryBuilder withCreatedOn(Timestamp createdOn) {
			this.createdOn = createdOn;
			return this;
		}

		public FileEntryBuilder withLastModifiedOn(Timestamp lastModifidedOn) {
			this.lastModifiedOn = lastModifidedOn;
			return this;
		}

		public FileEntryBuilder withLastModifiedBy(String lastModifiedBy) {
			this.lastModifiedBy = lastModifiedBy;
			return this;
		}

		public FileEntry build() {
			return new FileEntry(this);
		}

	}
}
