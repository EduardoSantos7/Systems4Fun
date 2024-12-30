from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from database import MongoDB, ProductsDB, CartsDB

app = FastAPI()

# Initialize database connections
products_db = ProductsDB(MongoDB(db_name="shop", collection_name="products", host="mongodb"))
carts_db = CartsDB(MongoDB(db_name="shop", collection_name="cart", host="mongodb"))

class Product(BaseModel):
    id: int
    name: str
    price: float

class CartItem(BaseModel):
    product_id: int
    quantity: int

@app.get("/products", response_model=List[Product])
def get_products():
    return products_db.get_products()

@app.post("/products")
def init_products(products: List[Product]):
    for product in products:
        products_db.add_product(product.dict())
    return {"message": "Products initialized"}

@app.post("/cart")
def add_to_cart(item: CartItem):
    product = products_db.get_products({"id": item.product_id})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    carts_db.add_to_cart({"product_id": item.product_id, "quantity": item.quantity})
    return {"message": "Product added to cart"}

@app.post("/checkout")
def checkout():
    cart_items = carts_db.get_cart_items()
    if not cart_items:
        raise HTTPException(status_code=400, detail="Cart is empty")
    total = sum(item["quantity"] * products_db.get_products({"id": item["product_id"]})[0]["price"] for item in cart_items)
    carts_db.db.collection.delete_many({})  # Clear the cart
    return {"message": "Purchase completed", "total": total}