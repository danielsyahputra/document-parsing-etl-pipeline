import io
import os
import logging
from PIL import Image
from minio import Minio
from minio.error import S3Error
from typing import Optional, Tuple

class MinioRepository:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = True
    ):
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)
        
        # Ensure bucket exists
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except S3Error as e:
            self.logger.error(f"Error ensuring bucket exists: {str(e)}")
            raise
    
    def save_image(
        self,
        image_data: bytes,
        document_id: int,
        chart_id: int,
        content_type: str = "image/png"
    ) -> str:
        try:
            object_name = f"documents/{document_id}/charts/{chart_id}.png"
            
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=io.BytesIO(image_data),
                length=len(image_data),
                content_type=content_type
            )
            
            return object_name
        except S3Error as e:
            self.logger.error(f"Error saving image to MinIO: {str(e)}")
            raise
    
    def get_image(
        self,
        document_id: int,
        chart_id: int
    ) -> Tuple[bytes, str]:
        try:
            object_name = f"documents/{document_id}/charts/{chart_id}.png"
            
            data = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            
            image_data = data.read()
            content_type = data.headers.get('content-type', 'image/png')
            
            return image_data, content_type
        except S3Error as e:
            self.logger.error(f"Error retrieving image from MinIO: {str(e)}")
            raise
    
    def delete_image(
        self,
        document_id: int,
        chart_id: int
    ) -> bool:
        try:
            object_name = f"documents/{document_id}/charts/{chart_id}.png"
            self.client.remove_object(
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            return True
        except S3Error as e:
            self.logger.error(f"Error deleting image from MinIO: {str(e)}")
            raise