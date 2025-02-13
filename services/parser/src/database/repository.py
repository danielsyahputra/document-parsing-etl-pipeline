import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from .schema import Document, DocumentChunk, ChartData

class DocumentRepository:
    def __init__(self, session: Session):
        self.session = session
        self.logger = logging.getLogger(__name__)

    def create_document(
        self,
        filename: str,
        chunks_data: List[Dict[str, Any]],
        charts_data: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        
        try:
            document = Document(
                filename=filename,
                total_chunks=len(chunks_data),
                metainfo=metadata or {}
            )
            self.session.add(document)
            self.session.flush()

            for idx, chunk_data in enumerate(chunks_data):
                chunk = DocumentChunk(
                    document_id=document.id,
                    chunk_index=idx,
                    text_content=chunk_data.get("text_content"),
                    entities=chunk_data.get("entities"),
                    chunk_metadata=chunk_data.get("metadata")
                )
                self.session.add(chunk)
            
            if charts_data:
                for chart_data in charts_data:
                    chart = ChartData(
                        document_id=document.id,
                        info=chart_data
                    )
                    self.session.add(chart)
            self.session.commit()
            return document.id
        
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error creating document: {str(e)}")
            raise

    def update_document(
        self,
        document_id: int,
        chunks_data: Optional[List[Dict[str, Any]]] = None,
        charts_data: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        try:
            document = self.session.query(Document).filter_by(id=document_id).first()
            if not document:
                raise ValueError(f"Document with ID {document_id} not found")

            if metadata is not None:
                document.metainfo = metadata
            
            if chunks_data is not None:
                self.session.query(DocumentChunk)\
                    .filter(DocumentChunk.document_id == document_id)\
                    .delete()
                
                for idx, chunk_data in enumerate(chunks_data):
                    chunk = DocumentChunk(
                        document_id=document_id,
                        chunk_index=idx,
                        text_content=chunk_data.get('text_content'),
                        entities=chunk_data.get('entities'),
                        chunk_metadata=chunk_data.get('metadata')
                    )
                    self.session.add(chunk)
                
                document.total_chunks = len(chunks_data)
            
            if charts_data is not None:
                self.session.query(ChartData)\
                    .filter(ChartData.document_id == document_id)\
                    .delete()

                for chart_data in charts_data:
                    chart = ChartData(
                        document_id=document_id,
                        info=chart_data
                    )
                    self.session.add(chart)

            self.session.commit()
            return True

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating document: {str(e)}")
            raise

    def get_document(self, document_id: int) -> Dict[str, Any]:
        try:
            document = self.session.query(Document).filter_by(id=document_id).first()
            if not document:
                raise ValueError(f"Document with ID {document_id} not found")

            return {
                'id': document.id,
                'filename': document.filename,
                'metadata': document.metainfo,
                'total_chunks': document.total_chunks,
                'created_at': document.created_at,
                'updated_at': document.updated_at,
                'chunks': [
                    {
                        'chunk_index': chunk.chunk_index,
                        'text_content': chunk.text_content,
                        'entities': chunk.entities,
                        'metadata': chunk.chunk_metadata,
                        'created_at': chunk.created_at
                    }
                    for chunk in sorted(document.chunks, key=lambda x: x.chunk_index)
                ],
                'charts': [
                    {
                        'id': chart.id,
                        'info': chart.info,
                        'created_at': chart.created_at
                    }
                    for chart in document.charts
                ]
            }

        except Exception as e:
            self.logger.error(f"Error retrieving document: {str(e)}")
            raise

    def get_document_chunks(
        self,
        document_id: int,
        start_chunk: Optional[int] = None,
        end_chunk: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        try:
            query = self.session.query(DocumentChunk)\
                .filter(DocumentChunk.document_id == document_id)\
                .order_by(DocumentChunk.chunk_index)

            if start_chunk is not None:
                query = query.filter(DocumentChunk.chunk_index >= start_chunk)
            if end_chunk is not None:
                query = query.filter(DocumentChunk.chunk_index <= end_chunk)

            chunks = query.all()
            return [
                {
                    'chunk_index': chunk.chunk_index,
                    'text_content': chunk.text_content,
                    'entities': chunk.entities,
                    'metadata': chunk.chunk_metadata,
                    'created_at': chunk.created_at
                }
                for chunk in chunks
            ]

        except Exception as e:
            self.logger.error(f"Error retrieving chunks: {str(e)}")
            raise

    def delete_document(self, document_id: int) -> bool:
        try:
            document = self.session.query(Document).filter_by(id=document_id).first()
            if not document:
                raise ValueError(f"Document with ID {document_id} not found")

            self.session.delete(document)
            self.session.commit()
            return True

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error deleting document: {str(e)}")
            raise

if __name__=="__main__":
    from .db import PostgresDatabase
    db = PostgresDatabase(
        username="airflow",
        password="airflow",
        host="172.17.0.1",
        port=5432,
        db="danielsyahputra"
    )
    repository = DocumentRepository(session=db.get_session())
    doc_id = repository.create_document(
        filename="example.pdf",
        chunks_data=[
            {
                'text_content': 'Chapter 1 content...',
                'entities': {'organizations': ['Company A', 'Company B']},
                'metadata': {'section': 'introduction'}
            },
            {
                'text_content': 'Chapter 2 content...',
                'entities': {'locations': ['New York', 'London']},
                'metadata': {'section': 'methods'}
            }
        ],
        charts_data=[
            {
                'type': 'bar',
                'title': 'Sales Data',
                'data': {'x': [1, 2, 3], 'y': [10, 20, 30]}
            }
        ],
        metadata={'source': 'research_paper', 'author': 'John Doe'}
    )
    print(doc_id)

    print("="*100)

    document = repository.get_document(doc_id)
    print(document)
    repository.update_document(
        doc_id,
        metadata={"status": "processed"}
    )
    document = repository.get_document(doc_id)
    print(document)
    chunks = repository.get_document_chunks(doc_id, start_chunk=0, end_chunk=1)
    print(chunks)