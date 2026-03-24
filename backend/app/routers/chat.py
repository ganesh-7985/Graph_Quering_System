"""
Chat routes: /api/chat/*
"""

from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi.responses import StreamingResponse

from app.dependencies import get_llm_service
from app.services.llm_service import LLMService
from app.models.schemas import ChatRequest, ChatResponse, ClearHistoryResponse

router = APIRouter(prefix="/api/chat", tags=["chat"])


@router.post("", response_model=ChatResponse)
def chat(request: ChatRequest, llm_service: LLMService = Depends(get_llm_service)):
    if not request.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    result = llm_service.query(request.message)
    return ChatResponse(**result)


@router.get("/stream")
async def chat_stream(
    message: str = Query(..., min_length=1),
    llm_service: LLMService = Depends(get_llm_service),
):
    return StreamingResponse(
        llm_service.query_stream(message),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/history")
def chat_history(llm_service: LLMService = Depends(get_llm_service)):
    """Return conversation memory state."""
    return llm_service.get_history_info()


@router.post("/clear", response_model=ClearHistoryResponse)
def chat_clear(llm_service: LLMService = Depends(get_llm_service)):
    llm_service.clear_history()
    return ClearHistoryResponse(status="ok", message="Conversation history cleared")
