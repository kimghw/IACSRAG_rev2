# infra/events/handlers/handler_registry.py
from typing import Dict, Callable, Awaitable, Any
from schema import DocumentEventType
from .content_pdf_handler import handle_pdf_event
from .content_md_handler import handle_markdown_event  # 파일명 수정됨
from .content_json_handler import handle_json_event

# 핸들러 레지스트리
DOCUMENT_HANDLERS: Dict[DocumentEventType, Callable[[Dict[str, Any]], Awaitable[None]]] = {
    DocumentEventType.PDF: handle_pdf_event,
    DocumentEventType.MARKDOWN: handle_markdown_event,
    DocumentEventType.JSON: handle_json_event,
}

def get_handler(event_type: DocumentEventType) -> Callable[[Dict[str, Any]], Awaitable[None]]:
    """이벤트 타입에 해당하는 핸들러 반환"""
    handler = DOCUMENT_HANDLERS.get(event_type)
    if not handler:
        raise ValueError(f"No handler registered for event type: {event_type}")
    return handler