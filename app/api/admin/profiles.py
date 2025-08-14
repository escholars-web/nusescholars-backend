from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException
from datetime import datetime
import uuid
from typing import List
from app.models.profile import UploadCSVResponse, FlaggedProfile, EditFlaggedProfileRequest, EditFlaggedProfileResponse
from app.services.profile_service import process_csv
from app.database.supabase_client import supabase

router = APIRouter(prefix="/admin/profiles", tags=["admin profiles"])

@router.post("/upload-csv", response_model=UploadCSVResponse)
async def upload_csv(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    print("File Content-Type:", file.content_type)
    if file.content_type != "text/csv":
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    file_bytes = await file.read()
    upload_id = str(uuid.uuid4())
    submitted_at = datetime.utcnow()

    background_tasks.add_task(process_csv, file_bytes, upload_id)

    return UploadCSVResponse(uploadId=upload_id, status="processing", submittedAt=submitted_at)

# TODO: we need to call this immediately after uploading the csv to retrieve the flagged profiles; should it be nested?
@router.get("/flagged", response_model=List[FlaggedProfile])
async def get_flagged_profiles():
    response = supabase.table("flagged_profiles").select("*").execute()
    if response.error:
        raise HTTPException(status_code=500, detail="Failed to fetch flagged profiles")
    profiles = response.data
    return [FlaggedProfile(
        profileId=p["profile_id"],
        data=p["data"],
        issues=p.get("issues", [])
    ) for p in profiles]

@router.post("/{profileId}/edit", response_model=EditFlaggedProfileResponse)
async def edit_flagged_profile(profileId: str, payload: EditFlaggedProfileRequest):
    if profileId != payload.profileId:
        raise HTTPException(status_code=400, detail="Profile ID mismatch")

    # Update flagged profile data in Supabase
    response = supabase.table("flagged_profiles")\
        .update({"data": payload.updatedData, "updated_at": payload.submittedAt.isoformat(), "issues": []})\
        .eq("profile_id", profileId)\
        .execute()

    if response.error or response.count == 0:
        raise HTTPException(status_code=404, detail="Flagged profile not found")

    return EditFlaggedProfileResponse(status="success", profileId=profileId)