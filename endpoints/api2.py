#2nd api for validate table , inserts all rows which are valid and stops on invalid rows or end of file ,
# incase of invalid rows found , returns invalid row no + invalid col name + invalid reason 
import pandas as pd
import json
from fastapi import Depends, HTTPException, status, File, UploadFile,FastAPI,APIRouter
from pymongo import MongoClient
import json
from typing import Optional, Annotated, List, Union
from pydantic import BaseModel
import uvicorn
from fastapi import Request
from get_tables import get_collection
import io



router = APIRouter(
    prefix="/api",
    tags=["api2"],
    responses={404: {"description": "Not found"}},
)


class DataValidator:
    
    def validate_fields(self,client_data: List[dict], schema: dict):
       
        required_fields = schema
   
        type_map = {
            "string": str,
            "integer": int,
            "float": float,
            "boolean": bool,
            "array": list,
            "object": dict,
        }

        invalid_transactions = []
        for index, data in enumerate(client_data, start=1): 
            errors = []
            for field, field_type in required_fields.items():
                if field == "_id":
                    continue
                if field not in data:
                    errors.append(f"Missing field: {field}")
                  
                elif not isinstance(data[field], type_map.get(field_type, object)):
                    errors.append(
                        f"Field '{field}' has invalid type. Expected {field_type}, got {type(data[field]).__name__}."
                    )
                   
            if errors:
                invalid_transactions.append({"row": index, "entry": data, "errors": errors})
               

        return {
            "msg": "Validation completed",
            "invalid_transactions": invalid_transactions,
            "is_valid": len(invalid_transactions) == 0,
        }




async def upload_file(file: UploadFile = File(...)):
    file_extension = file.filename.split(".")[-1]

    if file_extension.lower() == "json":
        contents = await file.read()
        try:
            json_data = json.loads(contents.decode("utf-8"))
            return {"msg": "JSON file received", "data": json_data, "status_code": 200, "type": "json"}
        except json.JSONDecodeError:
            pass

    elif file_extension.lower() in ["csv", "xlsx"]:
        contents = await file.read()
        try:
            if file_extension.lower() == "csv":
                df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
                df = df.where(pd.notna(df), "")
                headers = df.columns.tolist()
                return {"msg": "CSV file received", "data": df.to_dict(orient='records'), "headers": headers,
                        "status_code": 200,
                        "type": "csv"}

            elif file_extension.lower() == "xlsx":
                df = pd.read_excel(io.BytesIO(contents))
                df = df.where(pd.notna(df), None)
                headers = df.columns.tolist()
                json_data = df.to_json(orient='records', date_format='iso', default_handler=str)
                return {"msg": "xlxs file received", "data": json.loads(json_data), "status_code": 200, "type": "xlsx", "headers": headers}


        except pd.errors.ParserError:
            pass

    return {"msg": "Unsupported file format, not csv/xlxs/json"}








@router.post('/add_client_data')
async def add_data_into_table(table_name: str, file: UploadFile = File(None)):
    try:
        # Fetch the collection and schema from database

        result = get_collection(table_name)
        collection = result["collection"]
        table_schema = result["schema"]

        print(table_schema)

        if not file:
            raise HTTPException(status_code=400, detail="File not provided.")

        response = await upload_file(file)
        if response.get("status_code") != 200:
            raise HTTPException(status_code=400, detail="File upload failed.")
        

        client_data = response.get("data", [])
        if not client_data:
            raise HTTPException(status_code=400, detail="Uploaded file contains no data.")


        validator = DataValidator()
        validate_response = validator.validate_fields(client_data, table_schema)
       


        if not validate_response.get("is_valid"):
            return {
                "msg": "Validation failed",
                "invalid_transactions": validate_response["invalid_transactions"],
            }

        print(client_data)
        inserted_ids = collection.insert_many(client_data).inserted_ids

        # inserted_ids = collection.insert_many([client_data]).inserted_ids

        return {
            "msg": "Data added successfully",
            "inserted_count": len(inserted_ids),
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
