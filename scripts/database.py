"""
Database layer for the Feldenflow application.

This module provides typed models and database operations using Pydantic
for data validation and PostgreSQL for storage.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union, Literal
import os
import json

import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, Field, validator, root_validator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Source type literals
SourceType = Literal["yt_video", "audio_file", "text_file"]


# Base models

class DBModel(BaseModel):
    """Base model for database entities with common methods."""
    
    class Config:
        """Pydantic configuration."""
        from_attributes = True
        arbitrary_types_allowed = True

    @classmethod
    def get_db_connection(cls):
        """Get database connection."""
        db_connection_string = os.getenv('DATABASE_URL')
        if not db_connection_string:
            raise ValueError("DATABASE_URL environment variable not set")
        return psycopg2.connect(db_connection_string)


# Provider models

class DocumentProvider(DBModel):
    """Document provider model."""
    id: Optional[int] = None
    name: str
    yt_name: Optional[str] = None
    yt_channel_id: Optional[str] = None
    yt_upload_active: Optional[bool] = None
    yt_upload_from_date: Optional[datetime] = None
    yt_private_channel: Optional[bool] = None
    
    @validator('yt_name', pre=True)
    def clean_yt_name(cls, v, values):
        """Clean YouTube name by removing @ symbol."""
        if v and isinstance(v, str):
            return v.replace('@', '')
        return v

    @classmethod
    def get_by_id(cls, provider_id: int) -> Optional['DocumentProvider']:
        """Get provider by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, name, yt_name, yt_channel_id, 
                       yt_upload_active::int::boolean as yt_upload_active, 
                       yt_upload_from_date,
                       yt_private_channel::int::boolean as yt_private_channel
                       FROM document_provider 
                       WHERE id = %s""",
                    (provider_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_all_active_youtube(cls) -> List['DocumentProvider']:
        """Get all active YouTube providers."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, name, yt_name, yt_channel_id,
                       yt_upload_active::int::boolean as yt_upload_active,
                       yt_upload_from_date,
                       yt_private_channel::int::boolean as yt_private_channel
                       FROM document_provider 
                       WHERE yt_upload_active = B'1'"""
                )
                records = cursor.fetchall()
                providers = []
                for record in records:
                    try:
                        providers.append(cls(**record))
                    except Exception as e:
                        print(f"Error creating provider from record {record}: {e}")
                return providers

    def save(self) -> 'DocumentProvider':
        """Save provider to database."""
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing provider
                    cursor.execute(
                        """UPDATE document_provider 
                           SET name = %s, yt_name = %s, yt_channel_id = %s, 
                           yt_upload_active = %s, yt_upload_from_date = %s,
                           yt_private_channel = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.name, self.yt_name, self.yt_channel_id, 
                         'B\'1\'' if self.yt_upload_active else 'B\'0\'' if self.yt_upload_active is not None else None,
                         self.yt_upload_from_date,
                         'B\'1\'' if self.yt_private_channel else 'B\'0\'' if self.yt_private_channel is not None else None,
                         self.id)
                    )
                else:
                    # Insert new provider
                    cursor.execute(
                        """INSERT INTO document_provider 
                           (name, yt_name, yt_channel_id, yt_upload_active, yt_upload_from_date, yt_private_channel) 
                           VALUES (%s, %s, %s, %s, %s, %s) 
                           RETURNING id""",
                        (self.name, self.yt_name, self.yt_channel_id, 
                         'B\'1\'' if self.yt_upload_active else 'B\'0\'' if self.yt_upload_active is not None else None,
                         self.yt_upload_from_date,
                         'B\'1\'' if self.yt_private_channel else 'B\'0\'' if self.yt_private_channel is not None else None)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                return self


# Video models

class YTVideoEntry(BaseModel):
    """YouTube video metadata."""
    id: str
    title: Optional[str] = None
    webpage_url: Optional[str] = None
    upload_date: Optional[str] = None
    uploader: Optional[str] = None
    duration: Optional[float] = None
    view_count: Optional[int] = None


class YTVideo(DBModel):
    """YouTube video model."""
    id: Optional[int] = None
    document_provider_id: int
    video_id: str
    upload_successful: bool = False
    retries_remaining: int = 3
    entry: YTVideoEntry
    create_date: Optional[datetime] = None
    update_date: Optional[datetime] = None

    @root_validator(pre=True)
    def parse_entry(cls, values):
        """Parse the entry field if it's a string."""
        entry = values.get('entry')
        if isinstance(entry, str):
            try:
                values['entry'] = json.loads(entry)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON in entry field")
        return values

    @classmethod
    def get_by_id(cls, video_id: int) -> Optional['YTVideo']:
        """Get video by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, document_provider_id, video_id, 
                       upload_successful::int::boolean as upload_successful, 
                       retries_remaining, entry, create_date, update_date
                       FROM yt_video 
                       WHERE id = %s""",
                    (video_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_by_video_id(cls, video_id: str) -> Optional['YTVideo']:
        """Get video by YouTube video ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, document_provider_id, video_id, 
                       upload_successful::int::boolean as upload_successful, 
                       retries_remaining, entry, create_date, update_date
                       FROM yt_video 
                       WHERE video_id = %s""",
                    (video_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_pending_downloads(cls, provider_id: Optional[int] = None) -> List['YTVideo']:
        """Get videos pending download."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                if provider_id:
                    cursor.execute(
                        """SELECT id, document_provider_id, video_id, 
                           upload_successful::int::boolean as upload_successful, 
                           retries_remaining, entry, create_date, update_date
                           FROM yt_video 
                           WHERE document_provider_id = %s 
                           AND upload_successful = B'0' 
                           AND retries_remaining > 0""",
                        (provider_id,)
                    )
                else:
                    cursor.execute(
                        """SELECT id, document_provider_id, video_id, 
                           upload_successful::int::boolean as upload_successful, 
                           retries_remaining, entry, create_date, update_date
                           FROM yt_video 
                           WHERE upload_successful = B'0' 
                           AND retries_remaining > 0"""
                    )
                records = cursor.fetchall()
                return [cls(**record) for record in records]

    def save(self) -> 'YTVideo':
        """Save video to database."""
        now = datetime.now(timezone.utc)
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing video
                    cursor.execute(
                        """UPDATE yt_video 
                           SET document_provider_id = %s, video_id = %s, 
                           upload_successful = %s, retries_remaining = %s,
                           entry = %s, update_date = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.document_provider_id, self.video_id,
                         'B\'1\'' if self.upload_successful else 'B\'0\'',
                         self.retries_remaining,
                         json.dumps(self.entry.dict()),
                         now, self.id)
                    )
                else:
                    # Insert new video
                    cursor.execute(
                        """INSERT INTO yt_video 
                           (document_provider_id, video_id, upload_successful, retries_remaining, 
                            entry, create_date, update_date) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s) 
                           RETURNING id""",
                        (self.document_provider_id, self.video_id,
                         'B\'1\'' if self.upload_successful else 'B\'0\'',
                         self.retries_remaining,
                         json.dumps(self.entry.dict()),
                         now, now)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                self.update_date = now
                if not self.create_date:
                    self.create_date = now
                return self


# Private video models

class YTPrivateChannelVideo(DBModel):
    """YouTube private channel video model."""
    id: Optional[int] = None
    name: str = ""
    video_id: str
    document_provider_id: int

    @classmethod
    def get_by_id(cls, video_id: int) -> Optional['YTPrivateChannelVideo']:
        """Get private video by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, name, video_id, document_provider_id
                       FROM yt_private_channel_video 
                       WHERE id = %s""",
                    (video_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_by_provider_id(cls, provider_id: int) -> List['YTPrivateChannelVideo']:
        """Get all private videos for a provider."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, name, video_id, document_provider_id
                       FROM yt_private_channel_video 
                       WHERE document_provider_id = %s""",
                    (provider_id,)
                )
                records = cursor.fetchall()
                return [cls(**record) for record in records]

    def save(self) -> 'YTPrivateChannelVideo':
        """Save private video to database."""
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing private video
                    cursor.execute(
                        """UPDATE yt_private_channel_video 
                           SET name = %s, video_id = %s, document_provider_id = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.name, self.video_id, self.document_provider_id, self.id)
                    )
                else:
                    # Insert new private video
                    cursor.execute(
                        """INSERT INTO yt_private_channel_video 
                           (name, video_id, document_provider_id) 
                           VALUES (%s, %s, %s) 
                           RETURNING id""",
                        (self.name, self.video_id, self.document_provider_id)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                return self


# Audio file models

class AudioFile(DBModel):
    """Audio file model."""
    id: Optional[int] = None
    file_path: str
    original_filename: str
    file_size_bytes: Optional[int] = None
    duration_seconds: Optional[float] = None
    source_type: Optional[SourceType] = None
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @root_validator(pre=True)
    def parse_metadata(cls, values):
        """Parse the metadata field if it's a string."""
        metadata = values.get('metadata')
        if isinstance(metadata, str):
            try:
                values['metadata'] = json.loads(metadata)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON in metadata field")
        elif metadata is None:
            values['metadata'] = {}
        return values

    @classmethod
    def get_by_id(cls, file_id: int) -> Optional['AudioFile']:
        """Get audio file by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, file_path, original_filename, file_size_bytes,
                       duration_seconds, source_type, metadata, created_at, updated_at
                       FROM audio_file 
                       WHERE id = %s""",
                    (file_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_all_without_transcription(cls) -> List['AudioFile']:
        """Get all audio files without transcription."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT af.id, af.file_path, af.original_filename, af.file_size_bytes,
                       af.duration_seconds, af.source_type, af.metadata, af.created_at, af.updated_at
                       FROM audio_file af
                       LEFT JOIN groq_job_chunk gjc ON gjc.source_type = 'audio_file' AND gjc.source_id = af.id
                       WHERE gjc.id IS NULL"""
                )
                records = cursor.fetchall()
                return [cls(**record) for record in records]

    def save(self) -> 'AudioFile':
        """Save audio file to database."""
        now = datetime.now(timezone.utc)
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing audio file
                    cursor.execute(
                        """UPDATE audio_file 
                           SET file_path = %s, original_filename = %s,
                           file_size_bytes = %s, duration_seconds = %s,
                           source_type = %s, metadata = %s, updated_at = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.file_path, self.original_filename,
                         self.file_size_bytes, self.duration_seconds,
                         self.source_type, json.dumps(self.metadata),
                         now, self.id)
                    )
                else:
                    # Insert new audio file
                    cursor.execute(
                        """INSERT INTO audio_file 
                           (file_path, original_filename, file_size_bytes, 
                            duration_seconds, source_type, metadata, created_at, updated_at) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                           RETURNING id""",
                        (self.file_path, self.original_filename,
                         self.file_size_bytes, self.duration_seconds,
                         self.source_type, json.dumps(self.metadata),
                         now, now)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                self.updated_at = now
                if not self.created_at:
                    self.created_at = now
                return self

# Text file models

class TextField(DBModel):
    """Text file model."""
    id: Optional[int] = None
    document_provider_id: int
    file_path: str
    original_filename: str
    file_size_bytes: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @root_validator(pre=True)
    def parse_metadata(cls, values):
        """Parse the metadata field if it's a string."""
        metadata = values.get('metadata')
        if isinstance(metadata, str):
            try:
                values['metadata'] = json.loads(metadata)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON in metadata field")
        elif metadata is None:
            values['metadata'] = {}
        return values

    @classmethod
    def get_by_id(cls, file_id: int) -> Optional['TextField']:
        """Get text file by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, document_provider_id, file_path, original_filename, file_size_bytes,
                       metadata, created_at, updated_at
                       FROM text_file 
                       WHERE id = %s""",
                    (file_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)
                
    @classmethod
    def get_by_provider_id(cls, provider_id: int) -> List['TextField']:
        """Get all text files for a provider."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, document_provider_id, file_path, original_filename, file_size_bytes,
                       metadata, created_at, updated_at
                       FROM text_file 
                       WHERE document_provider_id = %s""",
                    (provider_id,)
                )
                records = cursor.fetchall()
                return [cls(**record) for record in records]
                
    @classmethod
    def get_all_without_transcription(cls) -> List['TextField']:
        """Get all text files without transcription."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT tf.id, tf.document_provider_id, tf.file_path, tf.original_filename, 
                       tf.file_size_bytes, tf.metadata, tf.created_at, tf.updated_at
                       FROM text_file tf
                       LEFT JOIN transcription t ON t.source_type = 'text_file' AND t.source_id = tf.id
                       WHERE t.id IS NULL"""
                )
                records = cursor.fetchall()
                return [cls(**record) for record in records]

    def save(self) -> 'TextField':
        """Save text file to database."""
        now = datetime.now(timezone.utc)
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing text file
                    cursor.execute(
                        """UPDATE text_file 
                           SET document_provider_id = %s, file_path = %s, original_filename = %s,
                           file_size_bytes = %s, metadata = %s, updated_at = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.document_provider_id, self.file_path, self.original_filename,
                         self.file_size_bytes, json.dumps(self.metadata),
                         now, self.id)
                    )
                else:
                    # Insert new text file
                    cursor.execute(
                        """INSERT INTO text_file 
                           (document_provider_id, file_path, original_filename, file_size_bytes, 
                            metadata, created_at, updated_at) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s) 
                           RETURNING id""",
                        (self.document_provider_id, self.file_path, self.original_filename,
                         self.file_size_bytes, json.dumps(self.metadata),
                         now, now)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                self.updated_at = now
                if not self.created_at:
                    self.created_at = now
                return self


# Transcription models

class Transcription(DBModel):
    """Transcription model."""
    id: Optional[int] = None
    source_type: SourceType
    source_id: int
    transcription: str

    @classmethod
    def get_by_id(cls, transcription_id: int) -> Optional['Transcription']:
        """Get transcription by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, source_type, source_id, transcription
                       FROM transcription 
                       WHERE id = %s""",
                    (transcription_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_by_source(cls, source_type: SourceType, source_id: int) -> Optional['Transcription']:
        """Get transcription by source."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, source_type, source_id, transcription
                       FROM transcription 
                       WHERE source_type = %s AND source_id = %s""",
                    (source_type, source_id)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    def save(self) -> 'Transcription':
        """Save transcription to database."""
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing transcription
                    cursor.execute(
                        """UPDATE transcription 
                           SET source_type = %s, source_id = %s, transcription = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.source_type, self.source_id, self.transcription, self.id)
                    )
                else:
                    # Insert new transcription
                    cursor.execute(
                        """INSERT INTO transcription 
                           (source_type, source_id, transcription) 
                           VALUES (%s, %s, %s) 
                           RETURNING id""",
                        (self.source_type, self.source_id, self.transcription)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                return self


# Groq job models

# Define GroqJobStatus as a type alias
GroqJobStatus = Literal["pending", "submitted", "completed", "failed", "error"]


class GroqJobChunk(DBModel):
    """Groq job chunk model."""
    id: Optional[int] = None
    groq_job_id: int
    source_type: SourceType
    source_id: int
    chunk_index: int
    name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    s3_uri: Optional[str] = None
    transcription: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @classmethod
    def get_by_id(cls, chunk_id: int) -> Optional['GroqJobChunk']:
        """Get chunk by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, groq_job_id, source_type, source_id, chunk_index,
                       name, created_at, updated_at, s3_uri, transcription, start_time, end_time
                       FROM groq_job_chunk 
                       WHERE id = %s""",
                    (chunk_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_by_job_id(cls, job_id: int) -> List['GroqJobChunk']:
        """Get all chunks for a job."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, groq_job_id, source_type, source_id, chunk_index,
                       name, created_at, updated_at, s3_uri, transcription, start_time, end_time
                       FROM groq_job_chunk 
                       WHERE groq_job_id = %s
                       ORDER BY chunk_index""",
                    (job_id,)
                )
                records = cursor.fetchall()
                return [cls(**record) for record in records]

    @classmethod
    def get_by_source(cls, source_type: SourceType, source_id: int) -> List['GroqJobChunk']:
        """Get all chunks for a source."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, groq_job_id, source_type, source_id, chunk_index,
                       name, created_at, updated_at, s3_uri, transcription, start_time, end_time
                       FROM groq_job_chunk 
                       WHERE source_type = %s AND source_id = %s
                       ORDER BY chunk_index""",
                    (source_type, source_id)
                )
                records = cursor.fetchall()
                return [cls(**record) for record in records]

    @classmethod
    def get_completed_sources_without_transcription(cls) -> List[Dict[str, Any]]:
        """Get all sources with completed chunks without final transcription."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """WITH groq_finished AS (
                           SELECT 
                               source_type, 
                               source_id 
                           FROM 
                               groq_job_chunk 
                           GROUP BY 
                               source_type, 
                               source_id 
                           HAVING SUM(CASE WHEN transcription IS NOT NULL THEN 1 ELSE 0 END) = COUNT(*)
                       )
                       SELECT 
                           groq_finished.source_type,
                           groq_finished.source_id 
                       FROM 
                           groq_finished
                       LEFT JOIN 
                           transcription
                       ON 
                           groq_finished.source_type = transcription.source_type 
                           AND groq_finished.source_id = transcription.source_id
                       WHERE 
                           transcription.source_id IS NULL
                       ORDER BY 
                           groq_finished.source_type, 
                           groq_finished.source_id"""
                )
                return cursor.fetchall()

    def save(self) -> 'GroqJobChunk':
        """Save chunk to database."""
        now = datetime.now(timezone.utc)
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing chunk
                    cursor.execute(
                        """UPDATE groq_job_chunk 
                           SET groq_job_id = %s, source_type = %s, source_id = %s,
                           chunk_index = %s, name = %s, updated_at = %s,
                           s3_uri = %s, transcription = %s, start_time = %s, end_time = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.groq_job_id, self.source_type, self.source_id,
                         self.chunk_index, self.name, now,
                         self.s3_uri, self.transcription, self.start_time, self.end_time,
                         self.id)
                    )
                else:
                    # Insert new chunk
                    cursor.execute(
                        """INSERT INTO groq_job_chunk 
                           (groq_job_id, source_type, source_id, chunk_index, name, 
                            created_at, updated_at, s3_uri, transcription, start_time, end_time) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                           RETURNING id""",
                        (self.groq_job_id, self.source_type, self.source_id,
                         self.chunk_index, self.name, now, now,
                         self.s3_uri, self.transcription, self.start_time, self.end_time)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                self.updated_at = now
                if not self.created_at:
                    self.created_at = now
                return self


class GroqJob(DBModel):
    """Groq job model."""
    id: Optional[int] = None
    job_id: str
    status: GroqJobStatus
    groq_job_id: Optional[str] = None
    created_at: Optional[datetime] = None
    submitted_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    output: Optional[str] = None

    @classmethod
    def get_by_id(cls, job_id: int) -> Optional['GroqJob']:
        """Get job by ID."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, job_id, status, groq_job_id, created_at,
                       submitted_at, completed_at, error, output
                       FROM groq_job 
                       WHERE id = %s""",
                    (job_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    @classmethod
    def get_by_status(cls, status: GroqJobStatus) -> List['GroqJob']:
        """Get all jobs with specific status."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, job_id, status, groq_job_id, created_at,
                       submitted_at, completed_at, error, output
                       FROM groq_job 
                       WHERE status = %s
                       ORDER BY id""",
                    (status,)
                )
                records = cursor.fetchall()
                return [cls(**record) for record in records]

    @classmethod
    def get_earliest_by_status(cls, status: GroqJobStatus) -> Optional['GroqJob']:
        """Get earliest job with specific status."""
        with cls.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT id, job_id, status, groq_job_id, created_at,
                       submitted_at, completed_at, error, output
                       FROM groq_job 
                       WHERE status = %s
                       ORDER BY id ASC
                       LIMIT 1""",
                    (status,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                return cls(**record)

    def get_chunks(self) -> List[GroqJobChunk]:
        """Get all chunks for this job."""
        if not self.id:
            return []
        return GroqJobChunk.get_by_job_id(self.id)

    def save(self) -> 'GroqJob':
        """Save job to database."""
        now = datetime.now(timezone.utc)
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                if self.id:
                    # Update existing job
                    cursor.execute(
                        """UPDATE groq_job 
                           SET job_id = %s, status = %s, groq_job_id = %s,
                           submitted_at = %s, completed_at = %s, error = %s, output = %s
                           WHERE id = %s
                           RETURNING id""",
                        (self.job_id, self.status, self.groq_job_id,
                         self.submitted_at, self.completed_at, self.error, self.output,
                         self.id)
                    )
                else:
                    # Insert new job
                    cursor.execute(
                        """INSERT INTO groq_job 
                           (job_id, status, groq_job_id, created_at, submitted_at, completed_at, error, output) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                           RETURNING id""",
                        (self.job_id, self.status, self.groq_job_id,
                         now, self.submitted_at, self.completed_at, self.error, self.output)
                    )
                self.id = cursor.fetchone()[0]
                conn.commit()
                if not self.created_at:
                    self.created_at = now
                return self


# Database operations

def create_batch_for_source(source_type: SourceType, source_id: int) -> Optional[GroqJob]:
    """Create a new Groq job batch for a specific source."""
    # First check if source exists
    if source_type == "yt_video":
        video = YTVideo.get_by_id(source_id)
        if not video:
            return None
    elif source_type == "audio_file":
        audio = AudioFile.get_by_id(source_id)
        if not audio:
            return None
    else:
        return None
    
    # Create a new Groq job
    job_id = f"groq_job_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    job = GroqJob(job_id=job_id, status="pending")
    job = job.save()
    
    return job


def get_sources_ready_for_processing() -> List[Dict[str, Any]]:
    """Get all sources ready for processing (not yet processed)."""
    # Get video sources
    with DBModel.get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """SELECT yv.id as source_id, 'yt_video' as source_type, 
                   yv.video_id, dp.yt_channel_id
                   FROM yt_video yv
                   INNER JOIN document_provider dp ON yv.document_provider_id = dp.id
                   LEFT JOIN groq_job_chunk gjc ON gjc.source_type = 'yt_video' AND gjc.source_id = yv.id
                   WHERE yv.upload_successful = B'1' AND gjc.id IS NULL
                """
            )
            video_sources = cursor.fetchall()
            
            # Get audio file sources
            cursor.execute(
                """SELECT af.id as source_id, 'audio_file' as source_type, 
                   af.original_filename, af.file_path
                   FROM audio_file af
                   LEFT JOIN groq_job_chunk gjc ON gjc.source_type = 'audio_file' AND gjc.source_id = af.id
                   WHERE gjc.id IS NULL
                """
            )
            audio_sources = cursor.fetchall()
            
            return video_sources + audio_sources


def get_file_path_for_source(source_type: SourceType, source_id: int) -> Optional[str]:
    """Get the file path for a source."""
    if source_type == "yt_video":
        # For YouTube videos we need to look up the provider and video ID
        with DBModel.get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """SELECT yt_video.video_id, document_provider.yt_channel_id 
                       FROM yt_video 
                       INNER JOIN document_provider ON yt_video.document_provider_id = document_provider.id
                       WHERE yt_video.id = %s""",
                    (source_id,)
                )
                record = cursor.fetchone()
                if not record:
                    return None
                
                video_id = record["video_id"]
                channel_id = record["yt_channel_id"]
                storage_path = os.getenv('FILE_STORAGE_PATH')
                
                if not storage_path:
                    raise ValueError("FILE_STORAGE_PATH environment variable not set")
                
                return f"{storage_path}/yt/{channel_id}/{video_id}.mp3"
    
    elif source_type == "audio_file":
        # For audio files we just need to look up the file path
        audio = AudioFile.get_by_id(source_id)
        if not audio:
            return None
        return audio.file_path
    
    return None


def create_job_chunk(job: GroqJob, source_type: SourceType, source_id: int, 
                     chunk_index: int, name: str, s3_uri: Optional[str] = None,
                     start_time: Optional[float] = None, end_time: Optional[float] = None) -> GroqJobChunk:
    """Create a new job chunk."""
    chunk = GroqJobChunk(
        groq_job_id=job.id,
        source_type=source_type,
        source_id=source_id,
        chunk_index=chunk_index,
        name=name,
        s3_uri=s3_uri,
        start_time=start_time,
        end_time=end_time
    )
    return chunk.save()


def update_job_status(job: GroqJob, status: GroqJobStatus, 
                     groq_job_id: Optional[str] = None,
                     submitted_at: Optional[datetime] = None,
                     completed_at: Optional[datetime] = None,
                     error: Optional[str] = None,
                     output: Optional[str] = None) -> GroqJob:
    """Update job status and related fields."""
    job.status = status
    
    if groq_job_id:
        job.groq_job_id = groq_job_id
    
    if submitted_at:
        job.submitted_at = submitted_at
    
    if completed_at:
        job.completed_at = completed_at
    
    if error:
        job.error = error
    
    if output:
        job.output = output
    
    return job.save()


def merge_transcriptions(source_type: SourceType, source_id: int) -> Optional[Transcription]:
    """Merge all transcriptions for a source into a single transcription."""
    # Get all chunks for this source
    chunks = GroqJobChunk.get_by_source(source_type, source_id)
    
    if not chunks:
        return None
    
    # Sort chunks by index
    chunks.sort(key=lambda x: x.chunk_index)
    
    # Check if all chunks have transcriptions
    if not all(chunk.transcription for chunk in chunks):
        return None
    
    # Combine all transcriptions
    combined_text = "\n".join(chunk.transcription for chunk in chunks if chunk.transcription)
    
    # Create and save a new transcription
    transcription = Transcription(
        source_type=source_type,
        source_id=source_id,
        transcription=combined_text
    )
    
    return transcription.save()


# Additional functions for Groq batch operations

def get_source_file_info(source_type: SourceType, source_id: int) -> Optional[Dict]:
    """
    Get file information for a specific source
    
    Args:
        source_type: Type of source ("yt_video" or "audio_file")
        source_id: ID of the source
        
    Returns:
        Dictionary with file information or None if not found
    """
    if source_type == "yt_video":
        # Get the YouTube video
        video = YTVideo.get_by_id(source_id)
        if not video:
            return None
            
        # Get the provider
        provider = DocumentProvider.get_by_id(video.document_provider_id)
        if not provider:
            return None
            
        # Construct file path based on convention
        storage_path = os.getenv('FILE_STORAGE_PATH')
        if not storage_path:
            raise ValueError("FILE_STORAGE_PATH environment variable not set")
            
        file_path = f"{storage_path}/yt/{provider.yt_channel_id}/{video.video_id}.mp3"
        
        # Check if file exists
        if not os.path.exists(file_path):
            return None
            
        return {
            "id": video.id,
            "source_type": "yt_video",
            "source_id": video.id,
            "video_id": video.video_id,
            "yt_channel_id": provider.yt_channel_id,
            "local_path": file_path,
            "name": video.video_id
        }
        
    elif source_type == "audio_file":
        # Get the audio file
        audio = AudioFile.get_by_id(source_id)
        if not audio:
            return None
            
        # Check if file exists
        if not os.path.exists(audio.file_path):
            return None
            
        return {
            "id": audio.id,
            "source_type": "audio_file",
            "source_id": audio.id,
            "original_filename": audio.original_filename,
            "file_size_bytes": audio.file_size_bytes,
            "duration_seconds": audio.duration_seconds,
            "local_path": audio.file_path,
            "name": audio.original_filename
        }
    
    return None


    """
    Get sources that need transcription, grouped into batches
    
    Args:
        source_type: Optional type to filter sources
        batch_size: Size of each batch
        
    Returns:
        List of batches, where each batch is a list of source dictionaries
    """
    # Get sources ready for processing
    sources = get_sources_ready_for_processing()
    
    if source_type:
        sources = [s for s in sources if s["source_type"] == source_type]
    
    result = []
    
    # Group sources into batches
    for i in range(0, len(sources), batch_size):
        batch = sources[i:i+batch_size]
        source_infos = []
        
        for source in batch:
            source_info = get_source_file_info(source["source_type"], source["source_id"])
            if source_info:
                source_infos.append(source_info)
        
        if source_infos:
            result.append(source_infos)
    
    return result


def create_groq_job_with_chunks(chunks: List[Dict]) -> Optional[Dict]:
    """
    Create a Groq job with chunks
    
    Args:
        chunks: List of chunk dictionaries
        
    Returns:
        Dictionary with job information or None if creation fails
    """
    if not chunks:
        return None
    
    # Create a new Groq job
    job_id = f"groq_job_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{len(chunks)}_chunks"
    
    job = GroqJob(
        job_id=job_id,
        status="pending",
        created_at=datetime.now(timezone.utc)
    )
    job = job.save()
    
    # Create chunks
    for i, chunk in enumerate(chunks):
        job_chunk = GroqJobChunk(
            groq_job_id=job.id,
            source_type=chunk["source_type"],
            source_id=chunk["source_id"],
            chunk_index=chunk.get("chunk_index", i),
            name=chunk.get("name", f"chunk_{i}")
        )
        job_chunk.save()
    
    return {
        "id": job.id,
        "job_id": job.job_id,
        "status": job.status,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "chunks": chunks,
        "num_chunks": len(chunks)
    }


def get_groq_jobs_with_chunks_by_status(status: GroqJobStatus) -> List[Dict]:
    """
    Get Groq jobs with a specific status, including their chunks
    
    Args:
        status: Status to filter jobs
        
    Returns:
        List of dictionaries with job information
    """
    # Get jobs with the specified status
    jobs = GroqJob.get_by_status(status)
    result = []
    
    for job in jobs:
        # Get chunks for this job
        chunks = GroqJobChunk.get_by_job_id(job.id)
        
        # Add file paths
        chunk_infos = []
        for chunk in chunks:
            file_path = get_file_path_for_source(chunk.source_type, chunk.source_id)
            
            if file_path:
                chunk_infos.append({
                    "id": chunk.id,
                    "groq_job_id": chunk.groq_job_id,
                    "source_type": chunk.source_type,
                    "source_id": chunk.source_id,
                    "chunk_index": chunk.chunk_index,
                    "s3_uri": chunk.s3_uri,
                    "local_path": file_path,
                    "name": chunk.name
                })
        
        # Add to result if there are chunks
        if chunk_infos:
            result.append({
                "job": job,
                "chunks": chunk_infos
            })
    
    return result


def update_groq_job_after_submission(job_id: int, groq_response: Dict) -> Dict:
    """
    Update a Groq job after submitting to the Groq API
    
    Args:
        job_id: ID of the job
        groq_response: Response from the Groq API
        
    Returns:
        Dictionary with status information
    """
    # Get the job
    job = GroqJob.get_by_id(job_id)
    if not job:
        return {"status": "error", "message": "Job not found", "job_id": job_id}
    
    # Extract Groq job ID from response
    groq_job_id = groq_response.get("id")
    if not groq_job_id:
        return {"status": "error", "message": "No Groq job ID in response", "job_id": job_id}
    
    # Update the job
    job.groq_job_id = groq_job_id
    job.status = "submitted"
    job.submitted_at = datetime.now(timezone.utc)
    job.save()
    
    return {
        "status": "success",
        "job_id": job_id,
        "groq_job_id": groq_job_id,
        "submitted_at": job.submitted_at.isoformat() if job.submitted_at else None,
        "job_status": job.status
    }