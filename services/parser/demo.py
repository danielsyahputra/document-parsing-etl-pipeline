import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from PIL import Image
import io
import json

API_BASE_URL = "http://172.17.0.1:8000"

def format_datetime(dt_str):
    """Format datetime string for display."""
    dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def get_document_charts(document_id):
    """Fetch all charts for a specific document."""
    try:
        response = requests.get(f"{API_BASE_URL}/documents/{document_id}/charts")
        if response.status_code == 200:
            return response.json()
        return []
    except Exception as e:
        st.error(f"Error fetching charts: {str(e)}")
        return []

def get_chart_image(document_id, chart_id):
    """Fetch image data for a specific chart within a document."""
    try:
        response = requests.get(
            f"{API_BASE_URL}/documents/{document_id}/charts/{chart_id}"
        )
        if response.status_code == 200 and 'image_data' in response.json():
            return Image.open(io.BytesIO(response.json()['image_data']))
        return None
    except Exception as e:
        st.error(f"Error loading chart image: {str(e)}")
        return None

def display_entities(entities):
    """Display entities in a formatted way."""
    if not entities:
        return
    
    for entity_type, items in entities.items():
        if items:
            st.write(f"**{entity_type.title()}:**")
            if isinstance(items[0], dict):
                for item in items:
                    st.write(f"- {item['text']} ({item['label']})")
            else:
                st.write(", ".join(items))

def display_document_content(doc_id):
    """Display document content including chunks and charts."""
    try:
        response = requests.get(f"{API_BASE_URL}/documents/{doc_id}")
        if response.status_code == 200:
            doc_details = response.json()
            
            st.header("Document Information")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Filename:** {doc_details['document']['filename']}")
                st.write(f"**Total Chunks:** {doc_details['document']['total_chunks']}")
            with col2:
                st.write(f"**Created:** {format_datetime(doc_details['document']['created_at'])}")
                if doc_details['document']['metadata']:
                    st.write("**Metadata:**")
                    st.json(doc_details['document']['metadata'])
            
            tab1, tab2 = st.tabs(["Text Chunks", "Charts & Images"])
            
            with tab1:
                st.subheader("Document Chunks")
                for chunk in doc_details['chunks']:
                    with st.expander(f"Chunk {chunk['chunk_index']}"):
                        st.write("**Text Content:**")
                        st.write(chunk['text_content'])
                        
                        st.write("**Entities:**")
                        display_entities(chunk['entities'])
                        
                        if chunk['metadata']:
                            st.write("**Metadata:**")
                            st.json(chunk['metadata'])
            
            with tab2:
                st.subheader("Charts and Images")
                charts = get_document_charts(doc_id)
                if charts:
                    for chart in charts:
                        with st.expander(
                            f"{chart['info']['type'].title()} {chart['info']['index']}"
                        ):
                            st.write("**Chart Information:**")
                            st.json(chart['info'])
                            
                            image = get_chart_image(doc_id, chart['id'])
                            if image:
                                st.image(
                                    image,
                                    caption=f"{chart['info']['type'].title()} {chart['info']['index']}"
                                )
                            else:
                                st.warning("Image not available")
                else:
                    st.info("No charts or images found in this document.")
                    
    except Exception as e:
        st.error(f"Error loading document content: {str(e)}")

def main():
    st.title("Document Processing System")
    st.sidebar.title("Navigation")
    
    st.sidebar.header("Upload Document")
    uploaded_file = st.sidebar.file_uploader("Choose a PDF file", type=["pdf"])
    
    if uploaded_file:
        files = {"file": uploaded_file}
        response = requests.post(f"{API_BASE_URL}/documents/upload", files=files)
        if response.status_code == 200:
            st.sidebar.success("File uploaded successfully!")
            st.sidebar.info(
                "The document will be processed automatically by the watcher service."
            )
    
    try:
        response = requests.get(f"{API_BASE_URL}/documents")
        if response.status_code == 200:
            documents = response.json()
            
            if not documents:
                st.info("No processed documents found.")
                return
            
            doc_options = {
                f"{doc['filename']} (ID: {doc['id']})": doc['id']
                for doc in documents
            }
            selected_doc = st.selectbox(
                "Select a document to view",
                options=list(doc_options.keys())
            )
            
            if selected_doc:
                doc_id = doc_options[selected_doc]
                display_document_content(doc_id)
                
    except Exception as e:
        st.error(f"Error connecting to the API: {str(e)}")

if __name__ == "__main__":
    main()