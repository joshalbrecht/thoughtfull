"""
Slack API client for interacting with Slack workspaces.
"""
import os
from typing import Dict, List, Optional, Any

from loguru import logger
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class SlackClient:
    """Client for interacting with the Slack API."""

    def __init__(self, token: Optional[str] = None):
        """
        Initialize the Slack client.

        Args:
            token: Slack API token. If not provided, attempts to read from SLACK_API_TOKEN environment variable.
        """
        self.token = token or os.environ.get("SLACK_API_TOKEN")
        if not self.token:
            raise ValueError(
                "Slack API token not provided. Set SLACK_API_TOKEN environment variable or pass token to constructor."
            )
        self.client = WebClient(token=self.token)
        logger.debug("Initialized Slack client")

    @retry(
        retry=retry_if_exception_type(SlackApiError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def get_channel_messages(
        self, channel_id: str, limit: int = 100, oldest: Optional[str] = None, latest: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get messages from a channel.

        Args:
            channel_id: ID of the channel to fetch messages from
            limit: Maximum number of messages to return (default: 100)
            oldest: Start of time range to include messages (timestamp)
            latest: End of time range to include messages (timestamp)

        Returns:
            List of message objects
        """
        try:
            logger.debug(f"Fetching messages from channel {channel_id}")
            response = self.client.conversations_history(
                channel=channel_id,
                limit=limit,
                oldest=oldest,
                latest=latest,
            )
            messages = response["messages"]
            logger.info(f"Retrieved {len(messages)} messages from channel {channel_id}")
            return messages
        except SlackApiError as e:
            logger.error(f"Error fetching messages from channel {channel_id}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(SlackApiError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def get_thread_messages(self, channel_id: str, thread_ts: str) -> List[Dict[str, Any]]:
        """
        Get messages from a thread.

        Args:
            channel_id: ID of the channel containing the thread
            thread_ts: Timestamp of the parent message

        Returns:
            List of message objects in the thread
        """
        try:
            logger.debug(f"Fetching thread messages for thread {thread_ts} in channel {channel_id}")
            response = self.client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
            )
            messages = response["messages"]
            logger.info(f"Retrieved {len(messages)} messages from thread {thread_ts}")
            return messages
        except SlackApiError as e:
            logger.error(f"Error fetching thread messages for thread {thread_ts}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(SlackApiError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def get_user_info(self, user_id: str) -> Dict[str, Any]:
        """
        Get information about a user.

        Args:
            user_id: ID of the user

        Returns:
            User information object
        """
        try:
            logger.debug(f"Fetching user info for user {user_id}")
            response = self.client.users_info(user=user_id)
            return response["user"]
        except SlackApiError as e:
            logger.error(f"Error fetching user info for user {user_id}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type(SlackApiError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def get_channel_info(self, channel_id: str) -> Dict[str, Any]:
        """
        Get information about a channel.

        Args:
            channel_id: ID of the channel

        Returns:
            Channel information object
        """
        try:
            logger.debug(f"Fetching channel info for channel {channel_id}")
            response = self.client.conversations_info(channel=channel_id)
            return response["channel"]
        except SlackApiError as e:
            logger.error(f"Error fetching channel info for channel {channel_id}: {e}")
            raise