-- -------------------------------------------------------------
-- TablePlus 6.4.8(608)
--
-- https://tableplus.com/
--
-- Database: feld
-- Generation Time: 2025-05-19 16:39:05.2630
-- -------------------------------------------------------------


-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS yt_video_id_seq;

-- Table Definition
CREATE TABLE "public"."yt_video" (
    "id" int4 NOT NULL DEFAULT nextval('yt_video_id_seq'::regclass),
    "document_provider_id" int4,
    "video_id" text,
    "upload_successful" bit,
    "retries_remaining" int4,
    "entry" jsonb NOT NULL,
    "create_date" timestamp,
    "update_date" timestamp,
    PRIMARY KEY ("id")
);

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS document_provider_id_seq;

-- Table Definition
CREATE TABLE "public"."document_provider" (
    "id" int4 NOT NULL DEFAULT nextval('document_provider_id_seq'::regclass),
    "name" text NOT NULL,
    "yt_name" text,
    "yt_channel_id" text,
    "yt_upload_active" bit,
    "yt_upload_from_date" timestamptz,
    "yt_private_channel" bit,
    PRIMARY KEY ("id")
);

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS groq_job_id_seq;

-- Table Definition
CREATE TABLE "public"."groq_job" (
    "id" int4 NOT NULL DEFAULT nextval('groq_job_id_seq'::regclass),
    "job_id" text NOT NULL,
    "status" text NOT NULL,
    "groq_job_id" text,
    "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    "submitted_at" timestamp,
    "completed_at" timestamp,
    "error" text,
    "output" text,
    PRIMARY KEY ("id")
);

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS transcription_id_seq;

-- Table Definition
CREATE TABLE "public"."transcription" (
    "id" int4 NOT NULL DEFAULT nextval('transcription_id_seq'::regclass),
    "source_type" text,
    "source_id" int4,
    "transcription" text,
    PRIMARY KEY ("id")
);

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS fmtt_video_id_seq;

-- Table Definition
CREATE TABLE "public"."yt_private_channel_video" (
    "id" int4 NOT NULL DEFAULT nextval('fmtt_video_id_seq'::regclass),
    "name" text DEFAULT ''::text,
    "video_id" text NOT NULL DEFAULT '''''::t4ext'::text,
    "document_provider_id" int4 NOT NULL DEFAULT 0,
    PRIMARY KEY ("id")
);

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS audio_file_id_seq;

-- Table Definition
CREATE TABLE "public"."audio_file" (
    "id" int4 NOT NULL DEFAULT nextval('audio_file_id_seq'::regclass),
    "file_path" text NOT NULL,
    "original_filename" text NOT NULL,
    "file_size_bytes" int8,
    "duration_seconds" float8,
    "source_type" text,
    "metadata" jsonb,
    "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("id")
);

-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS groq_job_chunk_id_seq;

-- Table Definition
CREATE TABLE "public"."groq_job_chunk" (
    "id" int4 NOT NULL DEFAULT nextval('groq_job_chunk_id_seq'::regclass),
    "groq_job_id" int4,
    "source_type" text NOT NULL,
    "source_id" int4 NOT NULL,
    "chunk_index" int4 NOT NULL,
    "name" text NOT NULL,
    "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    "s3_uri" text,
    "transcription" text,
    "end_time" float8,
    "start_time" float8,
    PRIMARY KEY ("id")
);

ALTER TABLE "public"."yt_video" ADD FOREIGN KEY ("document_provider_id") REFERENCES "public"."document_provider"("id");


-- Indices
CREATE UNIQUE INDEX groq_job_job_id_key ON public.groq_job USING btree (job_id);
ALTER TABLE "public"."yt_private_channel_video" ADD FOREIGN KEY ("document_provider_id") REFERENCES "public"."document_provider"("id");


-- Indices
CREATE UNIQUE INDEX fmtt_video_pkey ON public.yt_private_channel_video USING btree (id);
CREATE UNIQUE INDEX unique_video_url ON public.yt_private_channel_video USING btree (video_id);
ALTER TABLE "public"."groq_job_chunk" ADD FOREIGN KEY ("groq_job_id") REFERENCES "public"."groq_job"("id") ON DELETE CASCADE;