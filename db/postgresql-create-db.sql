-- Table: file_queue

-- DROP TABLE file_queue;

CREATE TABLE file_queue
(
  id serial NOT NULL,
  file_name character varying(250) NOT NULL,
  status character varying(250) NOT NULL,
  version integer NOT NULL DEFAULT 0,
  updated_on timestamp with time zone,
  updated_by character varying(250),
  CONSTRAINT file_queue_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE file_queue
  OWNER TO postgres;
