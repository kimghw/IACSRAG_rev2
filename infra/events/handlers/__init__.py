# infra/events/handlers/__init__.py
from .handler_registry import DOCUMENT_HANDLERS, get_handler
from .content_pdf_handler import handle_pdf_event
from .content_md_handler import handle_markdown_event
from .content_json_handler import handle_json_event

__all__ = [
    'DOCUMENT_HANDLERS', 
    'get_handler',
    'handle_pdf_event',
    'handle_markdown_event', 
    'handle_json_event'
]