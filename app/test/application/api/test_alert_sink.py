import json
import logging

from fastapi.testclient import TestClient


def _post(client: TestClient, payload: dict):
    return client.post(
        '/ops/alerts',
        content=json.dumps(payload),
        headers={'Content-Type': 'application/json'},
    )


def _alert_log_records(caplog) -> dict:
    messages = (r.getMessage() for r in caplog.records)
    return {
        m.split()[1]: next(
            rec.levelname for rec in caplog.records if rec.getMessage() == m
        )
        for m in messages
        if m.startswith('ALERT ')
    }


def test_sink_rejects_get(client):
    # Only POST (Alertmanager webhook method) is defined on the sink route.
    assert client.get('/ops/alerts').status_code == 405


def test_sink_returns_204_on_valid_payload(client):
    payload = {
        'version': '4',
        'alerts': [
            {
                'status': 'firing',
                'labels': {
                    'alertname': 'OutboxRelayCircuitOpen',
                    'severity': 'warning',
                    'job': 'kafka-chat-api',
                },
                'annotations': {
                    'summary': 'test summary',
                    'runbook_url': 'docs/runbooks/kafka-outage.md',
                },
            },
        ],
    }
    assert _post(client, payload).status_code == 204


def test_sink_maps_severity_to_log_levels(client, caplog):
    # critical -> CRITICAL, warning -> WARNING; resolved alerts are INFO.
    payload = {
        'alerts': [
            {
                'status': 'firing',
                'labels': {'alertname': 'CriticalOne', 'severity': 'critical'},
                'annotations': {},
            },
            {
                'status': 'firing',
                'labels': {'alertname': 'WarnOne', 'severity': 'warning'},
                'annotations': {},
            },
            {
                'status': 'resolved',
                'labels': {'alertname': 'ResolvedOne', 'severity': 'critical'},
                'annotations': {},
            },
        ],
    }
    with caplog.at_level(logging.DEBUG, logger='application.api.ops.handlers'):
        assert _post(client, payload).status_code == 204
    by_name = _alert_log_records(caplog)
    assert by_name['CriticalOne'] == 'CRITICAL'
    assert by_name['WarnOne'] == 'WARNING'
    assert by_name['ResolvedOne'] == 'INFO'


def test_sink_unknown_severity_falls_back_to_warning(client, caplog):
    # The severity set is closed today (critical/warning); a new severity must
    # not make firing alerts invisible, so firing falls back to WARNING.
    payload = {
        'alerts': [
            {
                'status': 'firing',
                'labels': {'alertname': 'OddOne', 'severity': 'info'},
                'annotations': {},
            },
        ],
    }
    with caplog.at_level(logging.DEBUG, logger='application.api.ops.handlers'):
        assert _post(client, payload).status_code == 204
    assert _alert_log_records(caplog)['OddOne'] == 'WARNING'


def test_sink_message_includes_summary_and_runbook(client, caplog):
    payload = {
        'alerts': [
            {
                'status': 'firing',
                'labels': {'alertname': 'WithRunbook', 'severity': 'warning'},
                'annotations': {
                    'summary': 'breaker open',
                    'runbook_url': 'docs/runbooks/kafka-outage.md',
                },
            },
        ],
    }
    with caplog.at_level(logging.DEBUG, logger='application.api.ops.handlers'):
        _post(client, payload)
    messages = [r.getMessage() for r in caplog.records if r.getMessage().startswith('ALERT ')]
    assert any('breaker open' in m and 'docs/runbooks/kafka-outage.md' in m for m in messages)


def test_sink_malformed_body_returns_400(client):
    resp = client.post(
        '/ops/alerts',
        content='not-json',
        headers={'Content-Type': 'application/json'},
    )
    assert resp.status_code == 400
