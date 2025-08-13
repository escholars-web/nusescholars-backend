from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class UploadCSVResponse(BaseModel):
    uploadId: str
    status: str
    submittedAt: datetime

class FlaggedProfile(BaseModel):
    profileId: str
    data: Dict[str, Any]
    issues: List[str]

class EditFlaggedProfileRequest(BaseModel):
    profileId: str
    updatedData: Dict[str, Any]
    submittedAt: datetime

class EditFlaggedProfileResponse(BaseModel):
    status: str
    profileId: str