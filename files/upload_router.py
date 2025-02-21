import os
import uuid
import io
import hashlib
import asyncio
from typing import List, Dict, Optional

from fastapi import APIRouter, File, UploadFile, HTTPException, Depends, Body
from minio import Minio
from minio.error import S3Error
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from dotenv import load_dotenv

from src.db.uploadfiles_model import UploadedFile
from src.utils.session import get_async_db
from src.utils.base import AsyncSessionLocal
from src.utils.log import logger

load_dotenv()

router = APIRouter(tags=["file-upload"])

# MinIO configuration
minio_server = os.getenv("MINIO_SERVER")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_secure = os.getenv("MINIO_SECURE") == "True"
bucket_name = os.getenv("MINIO_BUCKET_NAME","molly")
# Initialize the MinIO client
minio_client = Minio(
    minio_server,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=minio_secure,
)

# Ensure MinIO bucket exists
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)


@router.post("/upload")
async def upload_attachments(
    files: List[UploadFile] = File(...),
    conversation_id: str = Body(...)
) -> Dict:
    """Upload multiple files to MinIO concurrently with independent sessions.

    Args:
        files (List[UploadFile]): List of files to upload.
        conversation_id (str): The conversation ID associated with the files.

    Returns:
        dict: A dictionary containing a message and list of file metadata or errors.
    """
    # Define a helper function to process each file with its own session
    sem = asyncio.Semaphore(10)
    async def process_file(file: UploadFile):
        async with sem, AsyncSessionLocal() as session:
            return await upload_attachment(file, conversation_id, session)

    tasks = [process_file(file) for file in files]
    attachment_info_list = await asyncio.gather(*tasks, return_exceptions=True)

    # 处理返回结果
    response = {
        "message": "Attachments processed",
        "attachments_info": []
    }
    has_errors = False
    for info in attachment_info_list:
        if isinstance(info, Exception):
            has_errors = True
            response["attachments_info"].append({"error": str(info)})
        else:
            response["attachments_info"].append(info)

    if has_errors:
        response["message"] = "Some attachments failed to process"
    return response



async def calculate_file_hash(file_data: bytes) -> str:
    """Calculate the SHA-256 hash of file content asynchronously.

    Args:
        file_data (bytes): The content of the file to hash.

    Returns:
        str: The hexadecimal SHA-256 hash of the file content.
    """
    loop = asyncio.get_event_loop()
    # Move synchronized hash calculations to the thread pool
    hash_value = await loop.run_in_executor(None, lambda: hashlib.sha256(file_data).hexdigest())
    return hash_value

async def insert_file_info_to_db(session: AsyncSession, conversation_id: str, file_info: dict) -> None:
    """Insert file metadata into the database.

    Args:
        session (AsyncSession): The active database session.
        conversation_id (str): The ID of the conversation associated with the file.
        file_info (dict): Metadata of the file to insert.

    Raises:
        HTTPException: If the database insertion fails.
    """
    try:
        uploaded_file = UploadedFile(
            id=file_info['file_id'],#file id
            conversation_id=conversation_id,#会话id
            file_name=file_info['file_name'],#文件名
            file_type=file_info['file_type'],#文件类型
            file_size=file_info['file_size'],#文件大小
            file_path=file_info['file_path'],#minio文件路径
            file_hash=file_info['file_hash'],#文件内容哈希值
            file_status=file_info['file_status'],#文件状态
            file_origin=file_info['file_origin']#用户上传
        )
        session.add(uploaded_file)
        await session.commit()
        logger.debug(f"Inserted file {file_info['file_name']} into database with status: {file_info['file_status']}")
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to insert file {file_info['file_name']} into database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database insert failed: {str(e)}")

async def check_existing_file(session: AsyncSession, conversation_id: str, file_hash: str) -> Optional[UploadedFile]:
    """Check if a file with the same hash exists in the given conversation.

    Args:
        session (AsyncSession): The active database session.
        conversation_id (str): The conversation ID to check.
        file_hash (str): The hash of the file content to search for.

    Returns:
        Optional[UploadedFile]: The existing file if found, otherwise None.
    """
    query = select(UploadedFile).where(
        UploadedFile.conversation_id == conversation_id,
        UploadedFile.file_hash == file_hash,
        UploadedFile.file_status == True , # 只检查成功上传的文件(包括agent输出，用户上传)
        UploadedFile.file_origin == 0
    )
    result = await session.execute(query)
    existing_file = result.scalars().first()
    return existing_file

async def upload_to_minio(bucket: str, object_name: str, data: io.BytesIO, length: int) -> None:
    """Upload a file to MinIO asynchronously.

    Args:
        bucket (str): The MinIO bucket name.
        object_name (str): The name of the object in MinIO.
        data (io.BytesIO): The file data to upload.
        length (int): The size of the file in bytes.

    Raises:
        S3Error: If the upload to MinIO fails.
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, minio_client.put_object, bucket, object_name, data, length)
    logger.debug(f"Completed async upload to MinIO: {object_name}")

async def upload_attachment(file: UploadFile, conversation_id: str, session: AsyncSession) -> Dict:
    """Upload a single file to MinIO asynchronously and store its metadata.

    Args:
        file (UploadFile): The file to upload.
        conversation_id (str): The conversation ID associated with the file.
        session (AsyncSession): The active database session.

    Returns:
        dict: Metadata of the uploaded file or information about an existing file.

    Raises:
        HTTPException: If file processing or MinIO upload fails.
    """
    file_id = str(uuid.uuid4())
    file_path = f"minio://{bucket_name}/{file_id}_{file.filename}"
    object_name = f"{file_id}_{file.filename}"
    try:
        # Read file content and calculate hash
        file_data = await file.read()
        file_hash = await calculate_file_hash(file_data)
        
        # Check if a file with the same content already exists in the current session
        if existing_file := await check_existing_file(session, conversation_id, file_hash):
            # If the file already exists, return a prompt message
            return {
                "message": "File content already exists in this conversation",
                "file_id": existing_file.id,
                "file_name": existing_file.file_name,
                "file_path": existing_file.file_path,
                "file_hash": existing_file.file_hash,
                "file_status": existing_file.file_status,
                "file_origin": existing_file.file_origin
            }
          
        # Prepare file metadata
        file_info = {
            "file_id": file_id,
            "file_name": file.filename,
            "file_size": len(file_data),
            "file_type": file.content_type,
            "file_path": file_path,
            "file_hash": file_hash,
            "file_status": False,  # 默认失败，成功时改为 True
            "file_origin": 0
        }

        # Upload to MinIO asynchronously
        await upload_to_minio(
            bucket_name,
            object_name,
            io.BytesIO(file_data),
            len(file_data),
        )
        file_info["file_status"] = True  # 上传成功
        logger.info(f"Uploaded file {file.filename} to MinIO successfully")

    except S3Error as minio_error:
        # MinIO 上传失败，记录日志并插入失败状态
        logger.error(f"MinIO upload failed for file {file.filename}: {str(minio_error)}")
        await insert_file_info_to_db(session, conversation_id, file_info)
        raise HTTPException(status_code=500, detail=f"MinIO upload failed: {str(minio_error)}")
    except Exception as e:
        # 其他异常（例如文件读取失败、哈希计算失败）
        logger.error(f"Unexpected error processing file {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"File processing failed: {str(e)}")
    # 上传成功，插入数据库
    await insert_file_info_to_db(session, conversation_id, file_info)
    return file_info
