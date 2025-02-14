from typing import List, Optional
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import shutil
import os
from pathlib import Path

from sqlalchemy.orm import Session
from src.database.db import PostgresDatabase
from src.database.repository import DocumentRepository, ChunkRepository, ChartRepository
from src.database.schema import Document, DocumentChunk, ChartData

class DocumentResponse(BaseModel):
    id: int
    filename: str
    total_chunks: int
    metadata: dict
    created_at: datetime
    updated_at: Optional[datetime]

class ChunkResponse(BaseModel):
    chunk_index: int
    text_content: str
    entities: dict
    metadata: dict
    created_at: datetime

class ChartResponse(BaseModel):
    id: int
    info: dict
    image_path: str
    created_at: datetime
    document_id: int

class DocumentDetailResponse(BaseModel):
    document: DocumentResponse
    chunks: List[ChunkResponse]
    charts: List[ChartResponse]

app = FastAPI(title="Document Processing API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_config = {
    "username": "airflow",
    "password": "airflow",
    "host": "172.17.0.1",
    "port": 5432,
    "db": "danielsyahputra"
}

db = PostgresDatabase(**db_config)
session = db.get_session()

document_repo = DocumentRepository(Document, session)
chunk_repo = ChunkRepository(DocumentChunk, session)
chart_repo = ChartRepository(ChartData, session)

UPLOAD_DIR = Path("/cache")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

@app.post("/documents/upload")
async def upload_document(file: UploadFile = File(...)):
    """Upload a document to be processed by the watcher service."""
    try:
        file_path = UPLOAD_DIR / file.filename
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        return {
            "message": "File uploaded successfully",
            "filename": file.filename,
            "status": "pending_processing"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents", response_model=List[DocumentResponse])
async def get_documents():
    """Get all processed documents."""
    try:
        documents = document_repo.get_multi()
        return [
            DocumentResponse(
                id=doc.id,
                filename=doc.filename,
                total_chunks=doc.total_chunks,
                metadata=doc.metainfo,
                created_at=doc.created_at,
                updated_at=doc.updated_at
            )
            for doc in documents
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents/{document_id}", response_model=DocumentDetailResponse)
async def get_document_details(document_id: int):
    """Get detailed information about a specific document."""
    try:
        doc_info = document_repo.get_with_relationships(document_id)
        if not doc_info:
            raise HTTPException(status_code=404, detail="Document not found")
        
        return DocumentDetailResponse(
            document=DocumentResponse(
                id=doc_info['id'],
                filename=doc_info['filename'],
                total_chunks=doc_info['total_chunks'],
                metadata=doc_info['metadata'],
                created_at=doc_info['created_at'],
                updated_at=doc_info['updated_at']
            ),
            chunks=[
                ChunkResponse(
                    chunk_index=chunk['chunk_index'],
                    text_content=chunk['text_content'],
                    entities=chunk['entities'],
                    metadata=chunk['metadata'],
                    created_at=chunk['created_at']
                )
                for chunk in doc_info['chunks']
            ],
            charts=[
                ChartResponse(
                    id=chart['id'],
                    info=chart['info'],
                    image_path=chart['image_path'],
                    created_at=chart['created_at'],
                    document_id=document_id
                )
                for chart in doc_info['charts']
            ]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents/{document_id}/chunks")
async def get_document_chunks(
    document_id: int,
    start_chunk: Optional[int] = None,
    end_chunk: Optional[int] = None
):
    """Get chunks for a specific document with optional range."""
    try:
        chunks = chunk_repo.get_document_chunks(
            document_id=document_id,
            start_chunk=start_chunk,
            end_chunk=end_chunk
        )
        return [
            {
                "chunk_index": chunk.chunk_index,
                "text_content": chunk.text_content,
                "entities": chunk.entities,
                "metadata": chunk.chunk_metadata
            }
            for chunk in chunks
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents/{document_id}/charts", response_model=List[ChartResponse])
async def get_document_charts(document_id: int):
    """Get all charts associated with a specific document."""
    try:
        doc_info = document_repo.get_with_relationships(document_id)
        if not doc_info:
            raise HTTPException(status_code=404, detail="Document not found")
        
        return [
            ChartResponse(
                id=chart['id'],
                info=chart['info'],
                image_path=chart['image_path'],
                created_at=chart['created_at'],
                document_id=document_id
            )
            for chart in doc_info['charts']
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/documents/{document_id}/charts/{chart_id}")
async def get_document_chart(document_id: int, chart_id: int):
    """Get specific chart information and image data for a document."""
    try:
        chart_info = chart_repo.get_with_image(chart_id)
        if not chart_info:
            raise HTTPException(status_code=404, detail="Chart not found")
        
        if chart_info['document_id'] != document_id:
            raise HTTPException(
                status_code=404,
                detail="Chart not found for this document"
            )
        
        return chart_info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)