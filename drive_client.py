from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


_DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]


class DriveConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class DriveFile:
    file_id: str
    name: str
    mime_type: str
    created_time: str | None


def _load_service_account_info() -> dict:
    json_env = os.getenv("CHAT_APP_DRIVE_SERVICE_ACCOUNT_JSON", "").strip()
    if json_env:
        return json.loads(json_env)
    b64_env = os.getenv("CHAT_APP_DRIVE_SERVICE_ACCOUNT_B64", "").strip()
    if b64_env:
        decoded = base64.b64decode(b64_env.encode("ascii"))
        return json.loads(decoded.decode("utf-8"))
    file_path = os.getenv("CHAT_APP_DRIVE_SERVICE_ACCOUNT_FILE", "").strip()
    if file_path:
        with open(file_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    raise DriveConfigError("Google Drive 서비스 계정 설정이 필요합니다.")


def get_drive_config_status() -> tuple[bool, str]:
    try:
        get_drive_folder_id()
        _load_service_account_info()
    except DriveConfigError as exc:
        return False, str(exc)
    return True, ""


def get_drive_folder_id() -> str:
    folder_id = os.getenv("CHAT_APP_DRIVE_FOLDER_ID", "").strip()
    if folder_id:
        return folder_id
    folder_url = os.getenv("CHAT_APP_DRIVE_FOLDER_URL", "").strip()
    if folder_url:
        parsed = _extract_drive_folder_id(folder_url)
        if parsed:
            return parsed
        raise DriveConfigError("CHAT_APP_DRIVE_FOLDER_URL에서 폴더 ID를 찾지 못했습니다.")
    raise DriveConfigError("CHAT_APP_DRIVE_FOLDER_ID 또는 CHAT_APP_DRIVE_FOLDER_URL이 필요합니다.")


def _extract_drive_folder_id(value: str) -> str | None:
    value = (value or "").strip()
    if not value:
        return None
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", value)
    if match:
        return match.group(1)
    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", value)
    if match:
        return match.group(1)
    return None


def _get_drive_service():
    info = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(info, scopes=_DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_drive_images(folder_id: str) -> list[DriveFile]:
    service = _get_drive_service()
    files: list[DriveFile] = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed=false and mimeType contains 'image/'"
    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, createdTime)",
                pageSize=200,
                pageToken=page_token,
            )
            .execute()
        )
        for item in resp.get("files", []):
            files.append(
                DriveFile(
                    file_id=str(item.get("id")),
                    name=str(item.get("name") or ""),
                    mime_type=str(item.get("mimeType") or "application/octet-stream"),
                    created_time=str(item.get("createdTime") or ""),
                )
            )
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def upload_drive_file(file_storage, folder_id: str) -> DriveFile:
    service = _get_drive_service()
    file_storage.stream.seek(0)
    mimetype = file_storage.mimetype or "application/octet-stream"
    filename = file_storage.filename or "upload"
    media = MediaIoBaseUpload(file_storage.stream, mimetype=mimetype, resumable=False)
    body = {"name": filename, "parents": [folder_id]}
    created = (
        service.files()
        .create(body=body, media_body=media, fields="id, name, mimeType, createdTime")
        .execute()
    )
    return DriveFile(
        file_id=str(created.get("id")),
        name=str(created.get("name") or filename),
        mime_type=str(created.get("mimeType") or mimetype),
        created_time=str(created.get("createdTime") or ""),
    )


def download_drive_file(file_id: str) -> tuple[bytes, str]:
    service = _get_drive_service()
    metadata = service.files().get(fileId=file_id, fields="mimeType").execute()
    mime_type = str(metadata.get("mimeType") or "application/octet-stream")
    content = service.files().get_media(fileId=file_id).execute()
    return content, mime_type


def delete_drive_file(file_id: str) -> None:
    service = _get_drive_service()
    service.files().delete(fileId=file_id).execute()
