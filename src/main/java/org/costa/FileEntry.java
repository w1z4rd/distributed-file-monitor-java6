package org.costa;

import static org.costa.FileEntryStatus.PENDING;

import java.sql.Timestamp;

public class FileEntry {
	private int id;
	private String name;
	private FileEntryStatus status;
	private String createdBy;
	private String lastModifiedBy;
	private Timestamp createdOn;
	private Timestamp lastModifiedOn;
	private Timestamp fileLastModifiedOn;

	public FileEntry() {

	}

	public FileEntry(String fileName, long fileLastModified, String createdBy, String modifiedBy) {
		long now = System.currentTimeMillis();
		this.id = -1;
		this.name = fileName;
		this.status = PENDING;
		this.lastModifiedBy = modifiedBy;
		this.createdBy = createdBy;
		this.lastModifiedOn = new Timestamp(now);
		this.createdOn = new Timestamp(now);
		this.fileLastModifiedOn = new Timestamp(fileLastModified);
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public FileEntryStatus getStatus() {
		return status;
	}

	public void setStatus(FileEntryStatus status) {
		this.status = status;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public String getLastModifiedBy() {
		return lastModifiedBy;
	}

	public void setLastModifiedBy(String lastModifiedBy) {
		this.lastModifiedBy = lastModifiedBy;
	}

	public Timestamp getCreatedOn() {
		return createdOn;
	}

	public void setCreatedOn(Timestamp createdOn) {
		this.createdOn = createdOn;
	}

	public Timestamp getLastModifiedOn() {
		return lastModifiedOn;
	}

	public void setLastModifiedOn(Timestamp lastModifiedOn) {
		this.lastModifiedOn = lastModifiedOn;
	}

	public Timestamp getFileLastModifiedOn() {
		return fileLastModifiedOn;
	}

	public void setFileLastModifiedOn(Timestamp fileLastModifiedOn) {
		this.fileLastModifiedOn = fileLastModifiedOn;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 17;
		result = prime * result + id;
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
				+ ", lastModifiedBy=" + lastModifiedBy + ", createdOn=" + createdOn + ", lastModifiedOn="
				+ lastModifiedOn + ", fileLastModifiedOn=" + fileLastModifiedOn + "]";
	}

}
