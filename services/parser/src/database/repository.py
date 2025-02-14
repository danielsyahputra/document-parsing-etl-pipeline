import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from .schema import Document, DocumentChunk, ChartData
from .base import BaseRepository
from ..storage.minio import MinioRepository

class DocumentRepository(BaseRepository[Document]):
    def __init__(self, model, session: Session):
        super().__init__(model, session)

    def create_with_chunks(
        self,
        filename: str,
        chunks_data: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Document:
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

            self.session.commit()
            return document
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error creating document with chunks: {str(e)}")
            raise

    def get_with_relationships(self, id: int) -> Optional[Dict[str, Any]]:
        try:
            document = self.get(id)
            if not document:
                return None

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
                        'created_at': chart.created_at,
                        'image_path': chart.image_path
                    }
                    for chart in document.charts
                ]
            }
        except Exception as e:
            self.logger.error(f"Error retrieving document with relationships: {str(e)}")
            raise

class ChunkRepository(BaseRepository[DocumentChunk]):
    def __init__(self, model, session: Session):
        super().__init__(model, session)

    def get_document_chunks(
        self,
        document_id: int,
        start_chunk: Optional[int] = None,
        end_chunk: Optional[int] = None
    ) -> List[DocumentChunk]:
        try:
            query = self.session.query(self.model)\
                .filter(self.model.document_id == document_id)\
                .order_by(self.model.chunk_index)

            if start_chunk is not None:
                query = query.filter(self.model.chunk_index >= start_chunk)
            if end_chunk is not None:
                query = query.filter(self.model.chunk_index <= end_chunk)

            return query.all()
        except Exception as e:
            self.logger.error(f"Error retrieving document chunks: {str(e)}")
            raise

class ChartRepository(BaseRepository[ChartData]):
    def __init__(self, model, session, minio_repository: Optional['MinioRepository'] = None):
        super().__init__(model, session)
        self.minio = minio_repository

    def create_with_image(
        self,
        document_id: int,
        chart_info: Dict[str, Any],
        image_data: Optional[bytes] = None
    ) -> ChartData:
        try:
            chart = ChartData(
                document_id=document_id,
                image_path=chart_info.get("image_path"),
                info=chart_info
            )
            self.session.add(chart)
            self.session.flush()

            if image_data and self.minio:
                image_path = self.minio.save_image(
                    image_data=image_data,
                    document_id=document_id,
                    chart_id=chart.id
                )
                chart.image_path = image_path

            self.session.commit()
            return chart
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error creating chart with image: {str(e)}")
            raise 

    def get_with_image(self, id: int) -> Optional[Dict[str, Any]]:
        try:
            chart = self.get(id)
            if not chart:
                return None

            result = {
                'id': chart.id,
                'document_id': chart.document_id,
                'info': chart.info,
                'created_at': chart.created_at,
                'image_path': chart.image_path
            }

            if chart.image_path and self.minio:
                image_data, content_type = self.minio.get_image(
                    document_id=chart.document_id,
                    chart_id=chart.id
                )
                result['image_data'] = image_data
                result['content_type'] = content_type

            return result
        except Exception as e:
            self.logger.error(f"Error retrieving chart with image: {str(e)}")
            raise

    def delete_with_image(self, id: int) -> bool:
        try:
            chart = self.get(id)
            if not chart:
                return False

            if chart.image_path and self.minio:
                self.minio.delete_image(
                    document_id=chart.document_id,
                    chart_id=chart.id
                )

            self.session.delete(chart)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error deleting chart with image: {str(e)}")
            raise



if __name__=="__main__":
    import io
    from datetime import datetime
    from PIL import Image, ImageDraw
    from .db import PostgresDatabase
    from ..storage.minio import MinioRepository

    def generate_dummy_image_pillow() -> bytes:
        image = Image.new('RGB', (400, 300), 'white')
        draw = ImageDraw.Draw(image)
        
        # Draw some shapes
        draw.rectangle([50, 50, 200, 200], outline='blue')
        draw.ellipse([250, 100, 350, 200], outline='green')
        draw.line([100, 250, 300, 250], fill='red', width=2)
        
        # Save to bytes
        buf = io.BytesIO()
        image.save(buf, format='PNG')
        return buf.getvalue()

    db = PostgresDatabase(
        username="airflow",
        password="airflow",
        host="172.17.0.1",
        port=5432,
        db="danielsyahputra"
    )
    minio_config = {
        "endpoint": "localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket_name": "documents",
        "secure": False
    }

    document_repo = DocumentRepository(model=Document, session=db.get_session())
    chunk_repo = ChunkRepository(model=DocumentChunk, session=db.get_session())
    chart_repo = ChartRepository(
        model=ChartData,
        session=db.get_session(),
        minio_repository=MinioRepository(**minio_config)
    )

    chunks_data = [
        {
            'text_content': 'Executive Summary\nThis report analyzes the performance...',
            'entities': {
                'organizations': ['TechCorp', 'InnovateInc'],
                'dates': ['2024-02-13', '2024-03-01']
            },
            'metadata': {'section': 'summary', 'page': 1}
        },
        {
            'text_content': 'Market Analysis\nThe technology sector has shown...',
            'entities': {
                'markets': ['AI', 'Cloud Computing'],
                'locations': ['Silicon Valley', 'New York']
            },
            'metadata': {'section': 'analysis', 'page': 2}
        },
        {
            'text_content': 'Financial Results\nQ4 2023 showed strong growth...',
            'entities': {
                'metrics': ['Revenue', 'Profit Margin'],
                'values': ['$10M', '25%']
            },
            'metadata': {'section': 'financials', 'page': 3}
        }
    ]
    
    chart_images = [
        generate_dummy_image_pillow(),
        generate_dummy_image_pillow(),
        generate_dummy_image_pillow()
    ]

    charts_data = [
        {
            'type': 'line',
            'title': 'Revenue Growth',
            'data': {
                'x': ['Jan', 'Feb', 'Mar', 'Apr'],
                'y': [100, 120, 150, 180]
            }
        },
        {
            'type': 'bar',
            'title': 'Market Share',
            'data': {
                'categories': ['Product A', 'Product B', 'Product C'],
                'values': [30, 45, 25]
            }
        },
        {
            'type': 'scatter',
            'title': 'Customer Distribution',
            'data': {
                'x': [1, 2, 3, 4, 5],
                'y': [2, 4, 3, 5, 4]
            }
        }
    ]

    try:
        # 1. Create document with chunks
        print("Creating document...")
        document = document_repo.create_with_chunks(
            filename="lalalal1.pdf",
            chunks_data=chunks_data,
            metadata={
                'source': 'quarterly_report',
                'author': 'Daniel Syahputra',
                'department': 'Finance',
                'created_date': datetime.now().isoformat()
            }
        )
        doc_id = document.id
        print(f"Created document with ID: {doc_id}")
        
        # 2. Add charts with images
        print("\nAdding charts...")
        for chart_data, image_data in zip(charts_data, chart_images):
            chart = chart_repo.create_with_image(
                document_id=doc_id,
                chart_info=chart_data,
                image_data=image_data
            )
            print(f"Created chart with ID: {chart.id}")
        
        # 3. Retrieve and display document
        print("\nRetrieving document...")
        document = document_repo.get_with_relationships(doc_id)
        print("\nDocument details:")
        print(f"Filename: {document['filename']}")
        print(f"Total chunks: {document['total_chunks']}")
        print(f"Metadata: {document['metadata']}")
        
        # 4. Display chunks
        print("\nChunks:")
        for chunk in document['chunks']:
            print(f"\nChunk {chunk['chunk_index']}:")
            print(f"Text: {chunk['text_content'][:100]}...")
            print(f"Entities: {chunk['entities']}")
        
        # 5. Display charts
        print("\nCharts:")
        for chart in document['charts']:
            print(f"\nChart ID: {chart['id']}")
            print(f"Info: {chart['info']}")
            print(f"Image path: {chart['image_path']}")
        
        # 6. Update document metadata
        print("\nUpdating document...")
        document_repo.update(
            id=doc_id,
            obj_in={
                'metainfo': {
                    'status': 'processed',
                    'processed_date': datetime.now().isoformat()
                }
            }
        )
        
        # 7. Retrieve specific chunks
        print("\nRetrieving specific chunks...")
        chunks = chunk_repo.get_document_chunks(
            document_id=doc_id,
            start_chunk=0,
            end_chunk=1
        )
        print(f"Retrieved {len(chunks)} chunks")
        
        # 8. Clean up (optional)
        print("\nCleaning up...")
        document_repo.delete(doc_id)
        print("Document deleted")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise