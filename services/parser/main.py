import logging
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import fitz

from src.engine.pdf_parser import IntegratedDocumentProcessor, ProcessingConfig

class DocumentProcessingHandler(FileSystemEventHandler):
    """
    Handles file system events and processes new documents automatically.
    This class watches for new files and processes them using the IntegratedDocumentProcessor.
    """
    
    def __init__(
        self,
        processor: IntegratedDocumentProcessor,
        watch_path: str,
        supported_extensions: set = {'.pdf'},
        process_existing: bool = False
    ):
        """
        Initialize the document processing handler.
        
        Args:
            processor: IntegratedDocumentProcessor instance for document processing
            watch_path: Directory path to watch for new files
            supported_extensions: Set of supported file extensions (default: {'.pdf'})
            process_existing: Whether to process existing files in the directory
        """
        self.logger = logging.getLogger(__name__)
        self.processor = processor
        self.watch_path = Path(watch_path)
        self.supported_extensions = supported_extensions
        
        self.watch_path.mkdir(parents=True, exist_ok=True)
        
        if process_existing:
            self.process_existing_files()

    def process_existing_files(self):
        """Process any existing files in the watch directory."""
        self.logger.info(f"Processing existing files in {self.watch_path}")
        for file_path in self.watch_path.glob('*'):
            if file_path.suffix.lower() in self.supported_extensions:
                self.process_document(str(file_path))

    def on_created(self, event):
        """Handle file creation events."""
        if not event.is_directory:
            file_path = Path(event.src_path)
            if file_path.suffix.lower() in self.supported_extensions:
                self.process_document(event.src_path)

    def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Extract metadata from the document using PyMuPDF (fitz).
        
        Args:
            file_path: Path to the document file
            
        Returns:
            Dictionary containing document metadata
        """
        try:
            doc = fitz.open(file_path)
            metadata = doc.metadata
            
            metadata.update({
                "page_count": len(doc),
                "file_size": os.path.getsize(file_path),
                "extraction_date": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            
            doc.close()
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {str(e)}")
            return {}

    def process_document(self, file_path: str):
        """
        Process a single document.
        
        This method:
        1. Extracts metadata from the document
        2. Processes the document using the IntegratedDocumentProcessor
        3. Logs the results
        
        Args:
            file_path: Path to the document file
        """
        file_path = str(file_path)  # Convert Path to string if needed
        filename = os.path.basename(file_path)
        
        self.logger.info(f"Processing new document: {filename}")
        
        try:
            metadata = self.extract_metadata(file_path)
            
            document = self.processor.process_document(
                file_path=file_path,
                metadata=metadata
            )
            
            doc_info = self.processor.get_document_info(document.id)
            
            self.logger.info(f"Successfully processed document: {filename}")
            self.logger.info(f"Document ID: {doc_info['id']}")
            self.logger.info(f"Total chunks: {doc_info['total_chunks']}")
            self.logger.info(f"Total charts/images: {len(doc_info['charts'])}")
            
        except Exception as e:
            self.logger.error(f"Error processing document {filename}: {str(e)}")

class DocumentProcessingService:
    """
    Service that manages document processing operations.
    This class sets up and manages the file system observer and processor.
    """
    
    def __init__(
        self,
        watch_path: str,
        db_config: Dict[str, Any],
        minio_config: Dict[str, Any],
        processing_config: Optional[ProcessingConfig] = None
    ):
        """
        Initialize the document processing service.
        
        Args:
            watch_path: Directory to watch for new documents
            db_config: Database configuration
            minio_config: MinIO configuration
            processing_config: Optional document processing configuration
        """
        self.processor = IntegratedDocumentProcessor(
            db_config=db_config,
            minio_config=minio_config,
            processing_config=processing_config
        )
        
        self.event_handler = DocumentProcessingHandler(
            processor=self.processor,
            watch_path=watch_path,
            process_existing=True
        )
        
        self.observer = Observer()
        self.observer.schedule(
            self.event_handler,
            watch_path,
            recursive=False
        )

    def start(self):
        """Start the document processing service."""
        self.observer.start()
        logging.info(f"Started watching folder: {self.event_handler.watch_path}")

    def stop(self):
        """Stop the document processing service and clean up resources."""
        self.observer.stop()
        self.observer.join()
        self.processor.cleanup()
        logging.info("Document processing service stopped")

def main():
    """Main function to run the document processing service."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    db_config = {
        "username": "airflow",
        "password": "airflow",
        "host": "172.17.0.1",
        "port": 5432,
        "db": "danielsyahputra"
    }
    
    minio_config = {
        "endpoint": "172.17.0.1:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "bucket_name": "documents",
        "secure": False
    }
    
    processing_config = ProcessingConfig(
        spacy_model="en_core_web_sm",
        tokenizer="BAAI/bge-small-en-v1.5",
        image_resolution_scale=2.0
    )
    
    service = DocumentProcessingService(
        watch_path="/cache",
        db_config=db_config,
        minio_config=minio_config,
        processing_config=processing_config
    )
    
    try:
        service.start()
        
        # Keep the service running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Shutting down document processing service...")
    finally:
        service.stop()

if __name__ == "__main__":
    main()