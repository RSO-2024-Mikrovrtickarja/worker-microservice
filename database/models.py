from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel
from uuid import UUID, uuid4


class Image(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)

    file_name: str

    file_path: str

    uploaded_at: datetime

    # ID of the owning user.
    owned_by_user_id: UUID
