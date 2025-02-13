import fitz
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.pdf'):
            self.process_pdf(event.src_path)

    def process_pdf(self, file_path):
        filename = os.path.basename(file_path)
        print(f"\nProcessing new PDF: {filename}")
        
        try:
            # Open the PDF file
            doc = fitz.open(file_path)
            
            # Print basic info
            print(f"Document Info:")
            print(f"Number of pages: {len(doc)}")
            
            # Process each page
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text()
                print(f"\nPage {page_num + 1} content:")
                print("-" * 50)
                print(text)
            
            # Get metadata
            metadata = doc.metadata
            print("\nPDF Metadata:")
            for key, value in metadata.items():
                print(f"{key}: {value}")
                
            doc.close()
            
        except Exception as e:
            print(f"Error processing PDF {filename}: {str(e)}")

def main():
    path_to_watch = "/watch_folder"
    event_handler = PDFHandler()
    observer = Observer()
    observer.schedule(event_handler, path_to_watch, recursive=False)
    observer.start()
    
    print(f"Started watching folder: {path_to_watch}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()