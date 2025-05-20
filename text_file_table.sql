-- Sequence and defined type
CREATE SEQUENCE IF NOT EXISTS text_file_id_seq;

-- Table Definition
CREATE TABLE "public"."text_file" (
    "id" int4 NOT NULL DEFAULT nextval('text_file_id_seq'::regclass),
    "document_provider_id" int4 NOT NULL,
    "file_path" text NOT NULL,
    "original_filename" text NOT NULL,
    "file_size_bytes" int8,
    "metadata" jsonb,
    "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    "updated_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("id")
);

ALTER TABLE "public"."text_file" ADD FOREIGN KEY ("document_provider_id") REFERENCES "public"."document_provider"("id");