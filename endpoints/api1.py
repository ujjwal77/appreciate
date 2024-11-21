from fastapi import Depends, HTTPException, status, File, UploadFile,FastAPI
from pymongo import MongoClient
import json
from typing import Optional, Annotated, List, Union
from pydantic import BaseModel
import uvicorn
from fastapi import Request,APIRouter


router = APIRouter(
    prefix="/api",
    tags=["api1"],
    responses={404: {"description": "Not found"}},
)


client = MongoClient("mongodb://localhost:27017/")
db = client['dynamic_db'] 


def check_format(filedata: str):
    try:
        json.loads(filedata)
        return 'JSON'
    except ValueError:
        return 'CSV'



class ColumnSchema(BaseModel):
    column_name: str
    data_type: str
    is_unique: bool
    is_nullable: bool
    default_value: Optional[str] = None


class TableSchema(BaseModel):
    table_name: str
    columns: List[ColumnSchema]




def create_mongo_collection(table_data: TableSchema):
 

    collection = db[table_data.table_name]

    document = {}
    for column in table_data.columns:
        document[column.column_name] = column.default_value if column.default_value else None

    # initialize the collection
    collection.insert_one(document)

    return collection



@router.get("/demo/")
async def demo():
    return "hey ujjwal"


@router.post("/create_table")
async def create_dynamic_table(request: Request):
    try:

        filedata = await request.body()
        filedata_str = filedata.decode("utf-8")

        format_type = check_format(filedata_str)

        if format_type != 'JSON':
            raise HTTPException(status_code=400, detail="Invalid,not json data format")


        schema = json.loads(filedata_str)
        table_data = TableSchema(**schema)

        collection = create_mongo_collection(table_data)

        return {"message": f"Collection '{table_data.table_name}' created successfully"}

    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


