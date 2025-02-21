import json
import os

from dotenv import load_dotenv
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from src.api.api import upsert_conversation, insert_user_input_chat
from src.api.protocols import UserInput, FileInfo, FileGroup
from src.db.uploadfiles_model import UploadedFile
from src.file.upload_router import minio_client
from src.model.openai_engine import proxy_stream_generator
from src.utils.jwt_util import decode_vaild
from src.utils.log import logger
from src.utils.session import get_async_db

load_dotenv()

router = APIRouter(tags=["chat with file"])

# JWT 配置
SECRET_KEY = os.getenv("SECRET_KEY", "") # 用于签名和验证 JWT 的密钥
ALGORITHM = os.getenv("ALGORITHM", "HS256") # 加密算法

# 聊天接口
@router.post("/chat_with_files")
async def backend_chat_with_files(
    request: Request,
    db: AsyncSession = Depends(get_async_db)
) -> StreamingResponse:
    """代理聊天接口，支持文件上传信息，流式转发到目标服务器"""
    try:
        raw_body = await request.body()
        logger.info(f"Raw request body: {raw_body.decode('utf-8')}")
        
        body = await request.json()
        logger.info(f"Parsed JSON body: {json.dumps(body, ensure_ascii=False)}")
        
        user_input = UserInput(**body)
        logger.info(f"Validated model: {user_input.dict()}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {str(e)}")
        raise HTTPException(status_code=422, detail="Invalid JSON format")
    except ValidationError as e:
        logger.error(f"模型验证失败: {e.errors()}")
        raise HTTPException(status_code=422, detail=e.errors())
    except Exception as e:
        logger.exception("未捕获的异常:")
        raise HTTPException(status_code=500, detail="Internal server error")
    # 校验 token
    payload = await decode_vaild(user_input.system_token, SECRET_KEY, algorithms=[ALGORITHM])
    unionid: str = payload.get("sub")
    if unionid is None:
        raise HTTPException(status_code=401, detail="unionid不存在")

    conversation_id = user_input.conversation_id
    prompt = user_input.prompt

    # 更新 conversation 和插入用户输入
    await upsert_conversation(conversation_id, unionid, prompt)
    msg_id = await insert_user_input_chat(conversation_id, query=prompt)
    logger.info(f"Generated msg_id: {msg_id}")

    # 根据 conversation_id 从数据库查询所有文件
    uploaded_files = await db.execute(
        select(UploadedFile).where(
            UploadedFile.conversation_id == conversation_id,
            UploadedFile.file_status == True,
            UploadedFile.file_origin == 0 
        )
    )
    uploaded_files = uploaded_files.scalars().all()

    # 构造 file_list
    file_groups = []
    if uploaded_files:
        logger.info(f"Found {len(uploaded_files)} files for conversation_id: {conversation_id}")
        files = []
        for uploaded_file in uploaded_files:
            file_name, file_content = get_file_content(uploaded_file.file_path)
            if file_content:  # 检查内容是否非空
                logger.info(f"File: {file_name}, Content length: {len(file_content)}")
                files.append(FileInfo(file_name=file_name, file_content=file_content))
            else:
                logger.warning(f"Skipped file {file_name} due to empty content")
            logger.info(f"File: {file_name}, Content length: {len(file_content)}")
            files.append(FileInfo(file_name=file_name, file_content=file_content))
        if files:
            file_groups.append(FileGroup(conversation_id=conversation_id, files=files))
    else:
        logger.warning(f"No files found in DB for conversation_id: {conversation_id}")
    # 更新 user_input.file_list
    user_input.file_list = file_groups
    logger.info(f"Final file_list length: {len(file_groups)} for conversation_id: {conversation_id}")
    return StreamingResponse(
        proxy_stream_generator(user_input, msg_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        }
    )

# 获取文件内容
def get_file_content(file_path: str) -> tuple[str, str]:
    path = file_path.replace("minio://", "")
    bucket_name, object_name = path.split("/", 1)
    file_name = object_name  
    try:
        response = minio_client.get_object(bucket_name, object_name)
        file_content = response.read().decode("utf-8") 
    except Exception as e:
        logger.error(f"Error fetching file from MinIO: {e}")
        file_content = ""
    finally:
        response.close()
        response.release_conn()
    return file_name, file_content

