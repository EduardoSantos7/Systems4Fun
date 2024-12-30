from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

app = FastAPI()

# Datos en memoria
products = [
    {"id": 1, "name": "Product 1", "price": 10.0},
    {"id": 2, "name": "Product 2", "price": 20.0},
    {"id": 3, "name": "Product 3", "price": 30.0},
]

cart = []

class Product(BaseModel):
    id: int
    name: str
    price: float

class CartItem(BaseModel):
    product_id: int
    quantity: int

@app.get("/products", response_model=List[Product])
def get_products():
    return products

@app.post("/cart")
def add_to_cart(item: CartItem):
    product = next((p for p in products if p["id"] == item.product_id), None)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    cart.append({"product_id": item.product_id, "quantity": item.quantity})
    return {"message": "Product added to cart"}

@app.post("/checkout")
def checkout():
    if not cart:
        raise HTTPException(status_code=400, detail="Cart is empty")
    total = sum(item["quantity"] * next(p["price"] for p in products if p["id"] == item["product_id"]) for item in cart)
    cart.clear()
    return {"message": "Purchase completed", "total": total}