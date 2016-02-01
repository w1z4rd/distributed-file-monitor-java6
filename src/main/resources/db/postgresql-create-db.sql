-- Database: upload_test

-- DROP DATABASE upload_test;

CREATE DATABASE upload_test
  WITH OWNER = postgres
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       LC_COLLATE = 'en_US.UTF-8'
       LC_CTYPE = 'en_US.UTF-8'
       CONNECTION LIMIT = -1;

-- Table: file_queue

-- DROP TABLE file_queue;

CREATE TABLE file_queue
(
  id serial NOT NULL,
  file_name character varying(250) NOT NULL,
  status character varying(250) NOT NULL,
  file_last_modification_date timestamp without time zone,
  file_checksum bigint,
  last_modification_date timestamp without time zone,
  last_modified_by character varying(250),
  creation_date timestamp without time zone,
  created_by character varying(250),
  CONSTRAINT file_queue_pkey PRIMARY KEY (id),
  CONSTRAINT file_queue_unique UNIQUE (file_name, file_last_modification_date, file_checksum)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE file_queue
  OWNER TO postgres;
