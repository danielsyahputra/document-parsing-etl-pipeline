import logging
from typing import Generic, TypeVar, Type, Optional, List, Any, Dict
from sqlalchemy.orm import Session

ModelType = TypeVar("ModelType")

class BaseRepository(Generic[ModelType]):
    def __init__(self, model: Type[ModelType], session: Session):
        self.model = model
        self.session = session
        self.logger = logging.getLogger(self.__class__.__name__)

    def create(self, obj_in: Dict[str, Any]) -> ModelType:
        try:
            db_obj = self.model(**obj_in)
            self.session.add(db_obj)
            self.session.commit()
            return db_obj
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error creating {self.model.__name__}: {str(e)}")
            raise

    def get(self, id: int) -> Optional[ModelType]:
        try:
            return self.session.query(self.model).filter(self.model.id == id).first()
        except Exception as e:
            self.logger.error(f"Error retrieving {self.model.__name__}: {str(e)}")
            raise

    def get_multi(self, skip: int = 0, limit: int = 100) -> List[ModelType]:
        try:
            return self.session.query(self.model).offset(skip).limit(limit).all()
        except Exception as e:
            self.logger.error(f"Error retrieving multiple {self.model.__name__}: {str(e)}")
            raise

    def update(self, id: int, obj_in: Dict[str, Any]) -> Optional[ModelType]:
        try:
            db_obj = self.get(id)
            if not db_obj:
                return None
            
            for key, value in obj_in.items():
                setattr(db_obj, key, value)
            
            self.session.commit()
            return db_obj
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating {self.model.__name__}: {str(e)}")
            raise

    def delete(self, id: int) -> bool:
        try:
            db_obj = self.get(id)
            if not db_obj:
                return False
            
            self.session.delete(db_obj)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error deleting {self.model.__name__}: {str(e)}")
            raise