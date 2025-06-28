 infra/events/handlers/handler_registry.py
from typing import Dict, Callable, Awaitable
from schema import DocumentEventType
from .pdf_handler import handle_pdf_event
from .markdown_handler import handle_markdown_event
from .json_handler import handle_json_event

# 핸들러 레지스트리
DOCUMENT_HANDLERS: Dict[DocumentEventType, Callable[[Dict], Awaitable[None]]] = {
    DocumentEventType.PDF: handle_pdf_event,
    DocumentEventType.MARKDOWN: handle_markdown_event,
    DocumentEventType.JSON: handle_json_event,
}

def get_handler(event_type: DocumentEventType) -> Callable[[Dict], Awaitable[None]]:
    """이벤트 타입에 해당하는 핸들러 반환"""
    handler = DOCUMENT_HANDLERS.get(event_type)
    if not handler:
        raise ValueError(f"No handler registered for event type: {event_type}")
    return handler
