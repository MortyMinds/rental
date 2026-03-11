import sqlite3
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = os.path.join(os.path.dirname(__file__), "rental_data.db")

@app.get("/api/rentals")
def get_rentals(
    min_price: Optional[int] = Query(None),
    max_price: Optional[int] = Query(None),
    min_beds: Optional[float] = Query(None),
    min_baths: Optional[float] = Query(None),
    city: Optional[str] = Query(None),
    zip: Optional[str] = Query(None),
    property_type: Optional[List[str]] = Query(None),
    source: Optional[List[str]] = Query(None),
):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        query = "SELECT * FROM rentals WHERE 1=1"
        params = []
        
        if min_price is not None:
            query += " AND price >= ?"
            params.append(min_price)
        if max_price is not None:
            query += " AND price <= ?"
            params.append(max_price)
        if min_beds is not None:
            query += " AND beds >= ?"
            params.append(min_beds)
        if min_baths is not None:
            query += " AND baths >= ?"
            params.append(min_baths)
        if city:
            query += " AND LOWER(city) LIKE ?"
            params.append(f"%{city.lower()}%")
        if zip:
            query += " AND zip = ?"
            params.append(zip)
        if property_type:
            placeholders = ','.join(['?'] * len(property_type))
            query += f" AND LOWER(property_type) IN ({placeholders})"
            params.extend([pt.lower() for pt in property_type])
        if source:
            placeholders = ','.join(['?'] * len(source))
            query += f" AND LOWER(source) IN ({placeholders})"
            params.extend([s.lower() for s in source])
            
        query += " ORDER BY first_seen DESC"
        
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        return {"error": str(e)}

frontend_dist = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")
if os.path.exists(frontend_dist):
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")
