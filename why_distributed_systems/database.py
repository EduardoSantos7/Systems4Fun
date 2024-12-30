from abc import ABC, abstractmethod
from typing import List, Dict, Any
from pymongo import MongoClient

class DatabaseInterface(ABC):
    @abstractmethod
    def create(self, data: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def read(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def update(self, query: Dict[str, Any], data: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def delete(self, query: Dict[str, Any]) -> Any:
        pass

class MongoDB(DatabaseInterface):
    def __init__(self, db_name: str, collection_name: str, host: str = "localhost"):
        self.client = MongoClient(f"mongodb://{host}:27017/")
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def create(self, data: Dict[str, Any]) -> Any:
        return self.collection.insert_one(data).inserted_id

    def read(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(self.collection.find(query))

    def update(self, query: Dict[str, Any], data: Dict[str, Any]) -> Any:
        return self.collection.update_one(query, {"$set": data}).modified_count

    def delete(self, query: Dict[str, Any]) -> Any:
        return self.collection.delete_one(query).deleted_count

class ProductsDB:
    def __init__(self, db: DatabaseInterface):
        self.db = db

    def add_product(self, product: Dict[str, Any]) -> Any:
        return self.db.create(product)

    def get_products(self, query: Dict[str, Any] = {}) -> List[Dict[str, Any]]:
        return self.db.read(query)

    def update_product(self, query: Dict[str, Any], data: Dict[str, Any]) -> Any:
        return self.db.update(query, data)

    def delete_product(self, query: Dict[str, Any]) -> Any:
        return self.db.delete(query)

class CartsDB:
    def __init__(self, db: DatabaseInterface):
        self.db = db

    def add_to_cart(self, item: Dict[str, Any]) -> Any:
        return self.db.create(item)

    def get_cart_items(self, query: Dict[str, Any] = {}) -> List[Dict[str, Any]]:
        return self.db.read(query)

    def update_cart_item(self, query: Dict[str, Any], data: Dict[str, Any]) -> Any:
        return self.db.update(query, data)

    def delete_cart_item(self, query: Dict[str, Any]) -> Any:
        return self.db.delete(query)