from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException
from datetime import datetime
import uuid
from typing import List
from app.models.profile import UploadCSVResponse, FlaggedProfile, EditFlaggedProfileRequest, EditFlaggedProfileResponse
from app.services.profile_service import process_profiles_file
from app.database.supabase_client import supabase

router = APIRouter(prefix="/admin/profiles", tags=["admin profiles"])

@router.post("/upload-file", response_model=UploadCSVResponse)
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    print("File Content-Type:", file.content_type)
    allowed_types = [
        "text/csv",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ]

    if file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail="Only CSV or XLSX files allowed")

    file_bytes = await file.read()
    upload_id = str(uuid.uuid4())
    submitted_at = datetime.utcnow()

    background_tasks.add_task(process_profiles_file, file_bytes, file.filename, upload_id)

    return UploadCSVResponse(uploadId=upload_id, status="processing", submittedAt=submitted_at)

