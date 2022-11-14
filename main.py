import socket
from datetime import datetime
import sys
import logging
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse,FileResponse
from pydantic import BaseModel
from opencensus.ext.azure.log_exporter import AzureLogHandler
from config import appversion, app_insights_key


class Item(BaseModel):
    title: str
    timestamp: datetime


app = FastAPI()


@app.on_event("startup")
def startup_event():
    hostname = socket.gethostname()
    format_str = f'{appversion}@{hostname} says:' + \
        '%(asctime)s - %(levelname)-8s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(level=logging.INFO)
    formatter = logging.Formatter(format_str, date_format)
    rootlogger = logging.getLogger()
    handler = AzureLogHandler(
        connection_string=f'InstrumentationKey={app_insights_key}')
    handler.setFormatter(formatter)
    handler.setLevel(logging.ERROR)
    rootlogger.addHandler(handler)
    logging.info("Application start")


@app.on_event("shutdown")
def shutdown_event():
    logging.info("Application shutdown")


@app.get("/")
def read_root():
    return {"Widget": appversion}


@app.post("/cv/predict")
async def cv_predict(file: UploadFile = File(), payload: str = Form()):
    item = Item(title="result",timestamp=datetime.now())
    json_compatible_item_data = jsonable_encoder(item)
    return JSONResponse(content=json_compatible_item_data)


@app.post("/ai/predict")
async def ai_predict(file: UploadFile = File(), payload: str = Form()):
    return FileResponse("no_file.zip",media_type="application/zip")