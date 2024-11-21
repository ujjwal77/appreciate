from fastapi import APIRouter
from endpoints import api1,api2

router = APIRouter()
router.include_router(api1.router)
router.include_router(api2.router)