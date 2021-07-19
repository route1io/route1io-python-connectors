"""Slack

This module contains code for sending messages to Slack via webhooks.
"""

import json
from typing import Dict

import requests

def slack_message(message: str) -> None:
    """Send plain message to Slack

    Parameters
    ----------
    message : str
        Single string message to be sent to Slack
    """
    message_json = json.dumps({"text": message})
    _post_slack(data=message_json)

def slack_block_message(message: Dict[str, str]) -> None:
    """Send formatted block message to Slack

    Parameters
    ----------
    message : Dict[str, str]
        Dictionary containing the formatted block message to be sent to Slack
    """
    message_json = json.dumps(message)
    _post_slack(data=message_json)

def _post_slack(data: str) -> None:
    """Send POST request to Slack Webhook for message"""
    headers = {"Content-type": "application/json"}
    resp = requests.post(
        env.SLACK_WEBHOOK,
        headers=headers,
        data=data
    )