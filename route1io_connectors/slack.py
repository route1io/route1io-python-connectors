"""Slack

This module contains code for sending messages to Slack via webhooks.
"""

import json
from typing import Dict

import requests

def slack_message(message: str, webhook: str) -> None:
    """Send plain message to Slack

    Parameters
    ----------
    message : str
        Single string message to be sent to Slack
    webhook : str
        WebHook for sending the message to
    """
    message_json = json.dumps({"text": message})
    _post_slack(data=message_json, webhook=webhook)

def slack_block_message(message: Dict[str, str], webhook: str) -> None:
    """Send formatted block message to Slack

    Parameters
    ----------
    message : Dict[str, str]
        Dictionary containing the formatted block message to be sent to Slack
    webhook : str
        WebHook for sending the message to
    """
    message_json = json.dumps(message)
    _post_slack(data=message_json, webhook=webhook)

def _post_slack(data: str, webhook: str) -> None:
    """Send POST request to Slack Webhook for message"""
    headers = {"Content-type": "application/json"}
    resp = requests.post(
        webhook,
        headers=headers,
        data=data
    )