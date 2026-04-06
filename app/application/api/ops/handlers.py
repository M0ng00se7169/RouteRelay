import logging

from fastapi import (
    APIRouter,
    Request,
)
from fastapi.responses import (
    JSONResponse,
    Response,
)


logger = logging.getLogger(__name__)

router = APIRouter(tags=['Ops'])

# Severity → log level for fired alerts; resolutions are informational.
_LEVELS = {
    'critical': logging.CRITICAL,
    'warning': logging.WARNING,
}


@router.post('/ops/alerts', status_code=204)
async def alert_sink(request: Request) -> Response:
    """Alertmanager webhook sink (ADR-0007, Chunk 3).

    Alertmanager posts its notification payload here so every alert lands in
    the structured JSON logs (level WARNING/CRITICAL/INFO, logger
    `application.api.ops.handlers`) — greppable in Loki and visible in the
    dashboard's live log panel. This is the dev-stack transport: it replaces
    paging integrations until the Telegram receiver decision (ADR-0007
    Chunk 3) is made.

    Deliberately unauthenticated: the payload is not sensitive and the route
    is only reachable on the backend network (the host port mapping belongs
    to the API port, but nothing in the payload grants privileges). If this
    ever changes, add a shared-token check before parsing.
    """
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={'detail': {'error': 'body must be Alertmanager webhook JSON'}},
        )
    alerts = payload.get('alerts', []) if isinstance(payload, dict) else []
    for alert in alerts:
        labels = alert.get('labels', {})
        annotations = alert.get('annotations', {})
        if alert.get('status') == 'firing':
            level = _LEVELS.get(labels.get('severity', ''), logging.WARNING)
        else:
            level = logging.INFO
        name = labels.get('alertname', 'unknown')
        alert_status = alert.get('status', 'unknown')
        message = f"ALERT {name} {alert_status}: {annotations.get('summary', '')}"
        runbook = annotations.get('runbook_url')
        if runbook:
            message += f' (runbook: {runbook})'
        logger.log(level, message)
    return Response(status_code=204)
