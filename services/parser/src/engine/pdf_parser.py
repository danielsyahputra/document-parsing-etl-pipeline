import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import time
import io
from PIL import Image
import spacy
import spacy.cli

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, InputFormat
from docling_core.types.doc import ImageRefMode, PictureItem, TableItem
from docling.chunking import HybridChunker

from ..database.db import PostgresDatabase
from ..database.schema import Document, DocumentChunk, ChartData
from ..database.repository import DocumentRepository, ChunkRepository, ChartRepository
from ..storage.minio import MinioRepository

@dataclass
class ProcessingConfig:
    """Configuration for document processing."""
    spacy_model: str = "en_core_web_sm"
    tokenizer: str = "BAAI/bge-small-en-v1.5"
    chunk_overlap: int = 0
    min_chunk_size: int = 100
    max_chunk_size: int = 2000
    image_resolution_scale: float = 2.

class IntegratedDocumentProcessor:
    """
    Handles document processing, database storage, and image management.
    This class integrates document parsing, entity extraction, and storage operations.
    """
    
    def __init__(
        self,
        db_config: Dict[str, Any],
        minio_config: Dict[str, Any],
        processing_config: Optional[ProcessingConfig] = None
    ):
        """
        Initialize the integrated document processor.
        
        Args:
            db_config: Database configuration dictionary
            minio_config: MinIO configuration dictionary
            processing_config: Optional configuration for document processing
        """
        self.logger = logging.getLogger(__name__)
        self.processing_config = processing_config or ProcessingConfig()
        
        self.db = PostgresDatabase(**db_config)
        self.session = self.db.get_session()
        self.minio = MinioRepository(**minio_config)
        
        self.document_repo = DocumentRepository(Document, self.session)
        self.chunk_repo = ChunkRepository(DocumentChunk, self.session)
        self.chart_repo = ChartRepository(ChartData, self.session, self.minio)
        
        self.converter = self._setup_document_converter()
        self.chunker = HybridChunker(tokenizer=self.processing_config.tokenizer)
        
        # Initialize spaCy
        try:
            self.nlp = spacy.load(self.processing_config.spacy_model)
        except OSError:
            spacy.cli.download(self.processing_config.spacy_model)
            self.nlp = spacy.load(self.processing_config.spacy_model)

    def _setup_document_converter(self) -> DocumentConverter:
        """Create and configure the document converter with appropriate options."""
        pipeline_options = PdfPipelineOptions()
        pipeline_options.images_scale = self.processing_config.image_resolution_scale
        pipeline_options.generate_page_images = True
        pipeline_options.generate_picture_images = True

        return DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )

    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extract named entities from text using spaCy."""
        doc = self.nlp(text)
        entities = {
            "persons": [],
            "organizations": [],
            "dates": [],
            "locations": [],
            "misc": []
        }
        
        for ent in doc.ents:
            if ent.label_ == "PERSON":
                entities["persons"].append(ent.text)
            elif ent.label_ == "ORG":
                entities["organizations"].append(ent.text)
            elif ent.label_ == "DATE":
                entities["dates"].append(ent.text)
            elif ent.label_ == "GPE":
                entities["locations"].append(ent.text)
            else:
                entities["misc"].append({"text": ent.text, "label": ent.label_})
        
        return entities

    def _save_image_to_minio(
        self,
        image: Image.Image,
        document_id: int,
        image_type: str,
        index: int
    ) -> str:
        """Save an image to MinIO and return its path."""
        image_bytes = io.BytesIO()
        image.save(image_bytes, format="PNG")
        
        image_path = f"document_{document_id}/{image_type}_{index}.png"
        self.minio.save_image(
            image_data=image_bytes.getvalue(),
            document_id=document_id,
            chart_id=index
        )
        
        return image_path

    def _process_document_images(
        self,
        document_id: int,
        conv_result
    ) -> List[Dict[str, Any]]:
        """Process and extract all images from the document."""
        chart_info = []
        table_counter = 0
        figure_counter = 0
        
        for element, level in conv_result.document.iterate_items():
            try:
                if isinstance(element, TableItem):
                    table_counter += 1
                    image = element.get_image(conv_result.document)
                    image_path = self._save_image_to_minio(
                        image=image,
                        document_id=document_id,
                        image_type="table",
                        index=table_counter
                    )
                    
                    chart_info.append({
                        "type": "table",
                        "index": table_counter,
                        "image_path": image_path,
                        "metadata": {
                            "level": level,
                            "caption": element.caption if hasattr(element, 'caption') else None
                        }
                    })

                elif isinstance(element, PictureItem):
                    figure_counter += 1
                    image = element.get_image(conv_result.document)
                    image_path = self._save_image_to_minio(
                        image=image,
                        document_id=document_id,
                        image_type="figure",
                        index=figure_counter
                    )
                    
                    chart_info.append({
                        "type": "figure",
                        "index": figure_counter,
                        "image_path": image_path,
                        "metadata": {
                            "level": level,
                            "caption": element.caption if hasattr(element, 'caption') else None
                        }
                    })
                    
            except Exception as e:
                self.logger.error(f"Error processing image element: {str(e)}")
                continue
                
        return chart_info

    def _create_chunk_data(self, chunk) -> Dict[str, Any]:
        """Create a dictionary containing chunk data."""
        enriched_text = self.chunker.serialize(chunk=chunk)
        entities = self._extract_entities(enriched_text)
        
        return {
            "text_content": enriched_text,
            "entities": entities,
            "metadata": {
                "token_count": len(chunk.text.split()),
            }
        }

    def process_document(
        self,
        file_path: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Document:
        """
        Process a document and store it in the database with its chunks and images.
        
        Args:
            file_path: Path to the document file
            metadata: Optional metadata about the document
            
        Returns:
            Document object representing the stored document
        """
        try:
            start_time = time.time()
            
            conv_result = self.converter.convert(file_path)
            
            chunks = list(self.chunker.chunk(conv_result.document))
            chunks_data = [self._create_chunk_data(chunk) for chunk in chunks]
            
            document = self.document_repo.create_with_chunks(
                filename=Path(file_path).name,
                chunks_data=chunks_data,
                metadata=metadata
            )
            
            chart_info_list = self._process_document_images(document.id, conv_result)
            
            for chart_info in chart_info_list:
                self.chart_repo.create_with_image(
                    document_id=document.id,
                    chart_info={
                        "type": chart_info["type"],
                        "index": chart_info["index"],
                        "metadata": chart_info["metadata"]
                    },
                )
            
            processing_time = time.time() - start_time
            self.logger.info(
                f"Document processed in {processing_time:.2f} seconds. "
                f"Found {len(chunks_data)} chunks and {len(chart_info_list)} images."
            )
            
            return document
            
        except Exception as e:
            self.logger.error(f"Error processing document: {str(e)}")
            raise

    def get_document_info(self, document_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve complete information about a document."""
        return self.document_repo.get_with_relationships(document_id)

    def get_document_chunks(
        self,
        document_id: int,
        start_chunk: Optional[int] = None,
        end_chunk: Optional[int] = None
    ) -> List[DocumentChunk]:
        """Retrieve specific chunks from a document."""
        return self.chunk_repo.get_document_chunks(
            document_id=document_id,
            start_chunk=start_chunk,
            end_chunk=end_chunk
        )

    def get_chart_with_image(self, chart_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve a chart and its image data."""
        return self.chart_repo.get_with_image(chart_id)

    def cleanup(self):
        """Clean up resources and close connections."""
        try:
            self.db.disconnect()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

def main():
    """Example usage of the IntegratedDocumentProcessor."""
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Configuration
    db_config = {
        "username": "airflow",
        "password": "airflow",
        "host": "172.17.0.1",
        "port": 5432,
        "db": "danielsyahputra"
    }
    
    minio_config = {
        "endpoint": "localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket_name": "documents",
        "secure": False
    }
    
    # Initialize processor
    processor = IntegratedDocumentProcessor(
        db_config=db_config,
        minio_config=minio_config,
        processing_config=ProcessingConfig(
            spacy_model="en_core_web_sm",
            tokenizer="BAAI/bge-small-en-v1.5",
            image_resolution_scale=2.0
        )
    )
    
    try:
        # Process a document
        document = processor.process_document(
            file_path="/home/danielsyahputra/Documents/Personal/document-parsing-etl-pipeline/docs/AR for improved learnability.pdf",
            metadata={
                "author": "Daniel Syahputra",
                "department": "Engineering",
                "tags": ["technical", "report"]
            }
        )
        
        # Retrieve and display results
        doc_info = processor.get_document_info(document.id)
        
        print(f"\nDocument ID: {doc_info['id']}")
        print(f"Filename: {doc_info['filename']}")
        print(f"Total chunks: {doc_info['total_chunks']}")
        
        print("\nChunks:")
        for chunk in doc_info['chunks']:
            print(f"\nChunk {chunk['chunk_index']}:")
            print(f"Text: {chunk['text_content'][:100]}...")
            print(f"Entities: {chunk['entities']}")
        
        print("\nCharts and Images:")
        for chart in doc_info['charts']:
            print(f"\nChart ID: {chart['id']}")
            print(f"Info: {chart['info']}")
            print(f"Image path: {chart['image_path']}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        processor.cleanup()

if __name__ == "__main__":
    main()