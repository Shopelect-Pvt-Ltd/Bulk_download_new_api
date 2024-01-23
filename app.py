from fastapi import FastAPI, File, UploadFile, HTTPException
import os
from werkzeug.utils import secure_filename
from addstatus import process_s3_links_csv
import subprocess
import threading 

app = FastAPI()
CWD =  os.getcwd()
UPLOADER_FOLDER = f'{CWD}/csv_uploads'
ALLOWED_EXTS = ('csv')

if not os.path.exists(UPLOADER_FOLDER):
    os.makedirs(UPLOADER_FOLDER)

def allowed_file(file_name):
    return '.' in file_name and file_name.rsplit('.', 1)[1].lower() in ALLOWED_EXTS

def clean_dir():
    files = os.listdir(UPLOADER_FOLDER)
    for f in files:
        file_path = os.path.join(UPLOADER_FOLDER, f)
        print("Removing: ", file_path)
        os.remove(file_path)


@app.post("/upload_run")
async def upload_file(file: UploadFile = File(...)):
    clean_dir()
    if allowed_file(file.filename):
        file_path = os.path.join(UPLOADER_FOLDER, secure_filename(file.filename))
        
        with open(file_path, "wb") as f:
            f.write(file.file.read())
        command = f"python addstatus.py {file_path}"
        th = threading.Thread(target=process_s3_links_csv, args=(file_path,))
        th.start()
        print("Running: ", command)
        return {"message": "File uploaded successfully"}
    raise HTTPException(status_code=400, detail="Invalid File format")

