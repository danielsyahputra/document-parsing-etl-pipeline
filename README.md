# Document Parsing ETL Pipeline


# Document Processing System

[![Python 3.8+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.0-green.svg)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.42.0-red.svg)](https://streamlit.io/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache-yellow.svg)](https://opensource.org/license/apache-2-0)

> An intelligent document processing system that automatically extracts text, entities, and images from PDF documents. The system features automated document watching, database storage, and a user-friendly web interface.

## 🚀 Features

- **Automated Document Processing**
  - PDF text extraction and chunking
  - Named entity recognition using spaCy
  - Automatic image and chart extraction
  - Real-time document watching

- **Smart Storage**
  - Text and metadata in PostgreSQL
  - Images and charts in MinIO
  - Efficient data retrieval

- **User-Friendly Interface**
  - Web-based document upload
  - Interactive document viewer
  - Entity visualization
  - Image gallery

## 🛠️ Technologies

- **Backend**
  - Python 3.10+
  - FastAPI
  - SQLAlchemy
  - SpaCy
  - Docling
  - Apache Airflow

- **Storage**
  - PostgreSQL
  - MinIO

- **Frontend**
  - Streamlit
  - Pillow

## 📦 Installation

1. **Clone the repository**
```bash
git clone https://github.com/danielsyahputra/document-parsing-etl-pipeline.git document-processing

cd document-processing
```

2. **Run using docker compose**
```
docker compose up -d
```

## 🔍 API Documentation

### Document Endpoints

- **Upload Document**
  ```http
  POST /documents/upload
  ```

- **List Documents**
  ```http
  GET /documents
  ```

- **Get Document Details**
  ```http
  GET /documents/{document_id}
  ```

- **Get Document Chunks**
  ```http
  GET /documents/{document_id}/chunks
  ```

### Chart Endpoints

- **Get Document Charts**
  ```http
  GET /documents/{document_id}/charts
  ```

- **Get Specific Chart**
  ```http
  GET /documents/{document_id}/charts/{chart_id}
  ```

## 📝 Usage Example

```python
import requests

# API base URL
API_URL = "http://localhost:8000"

# Upload a document
with open("sample.pdf", "rb") as f:
    files = {"file": f}
    response = requests.post(f"{API_URL}/documents/upload", files=files)
    document_id = response.json()["id"]

# Get document details
document = requests.get(f"{API_URL}/documents/{document_id}").json()

# Get document charts
charts = requests.get(f"{API_URL}/documents/{document_id}/charts").json()
```


## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## 👏 Acknowledgments

- [Docling](https://github.com/DS4SD/docling) for document processing
- [spaCy](https://spacy.io/) for NLP capabilities
- [FastAPI](https://fastapi.tiangolo.com/) for the REST API
- [Streamlit](https://streamlit.io/) for the web interface

## 📬 Contact

Daniel Syahputra - dnlshp@gmail.com
Project Link: [https://github.com/danielsyahputra/document-parsing-etl-pipeline](https://github.com/danielsyahputra/document-parsing-etl-pipeline)