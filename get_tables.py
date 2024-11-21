from fastapi import Depends, HTTPException, status, File, UploadFile,FastAPI
from pymongo import MongoClient
import json
from typing import Optional, Annotated, List, Union
from pydantic import BaseModel
import uvicorn
from fastapi import Request


client = MongoClient("mongodb://localhost:27017/")
db = client["dynamic_db"]


def get_collection(table_name: str):
 
    if table_name not in db.list_collection_names():
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in the database.")
    
    collection = db[table_name]

    sample_document = collection.find_one()
    
    if not sample_document:
        raise HTTPException(status_code=404, detail=f"No documents found in table '{table_name}'.")
    
    # Infer schema from the sample document
    inferred_schema = {}
    for key, value in sample_document.items():
        inferred_schema[key] = type(value).__name__ 
    
    return {
        "collection": collection,
        "schema": inferred_schema
    }
