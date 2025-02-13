import pytz
from datetime import datetime
from sqlalchemy import Column, Integer, String, JSON, DateTime, Float, ForeignKey, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

def get_jakarta_time():
    return datetime.now(pytz.timezone("Asia/Jakarta"))

class DocumentChunk(Base):
    __tablename__ = "document_chunks"
    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents.id'), nullable=False)
    chunk_index = Column(Integer, nullable=False)
    text_content = Column(Text, nullable=True)
    entities = Column(JSON, nullable=True)
    chunk_metadata = Column(JSON, nullable=True)
    created_at = Column(
        DateTime,
        default=get_jakarta_time,
        nullable=False
    )
    document = relationship("Document", back_populates="chunks")

class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False)
    total_chunks = Column(Integer, default=0)
    metainfo = Column(JSON, nullable=True)
    updated_at = Column(
        DateTime,
        default=get_jakarta_time,
        onupdate=get_jakarta_time,
    )
    created_at = Column(
        DateTime,
        default=get_jakarta_time,
        nullable=False
    )
    chunks = relationship("DocumentChunk", back_populates="document", cascade="all, delete-orphan")
    charts = relationship("ChartData", back_populates="document", cascade="all, delete-orphan")

class ChartData(Base):
    __tablename__ = "chart_data"
    id = Column(Integer, primary_key=True)
    document_id = Column(Integer, ForeignKey('documents.id'), nullable=False)
    created_at = Column(
        DateTime,
        default=get_jakarta_time,
        nullable=False
    )
    info = Column(JSON, nullable=True)
    image_path = Column(String, nullable=True)
    document = relationship("Document", back_populates="charts")