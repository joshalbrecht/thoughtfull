"""
Data models for Slack messages and related entities.
"""
from datetime import datetime
from typing import Dict, List, Optional, Any

from pydantic import BaseModel, Field


class SlackUser(BaseModel):
    """Model representing a Slack user."""
    
    id: str
    name: str
    real_name: Optional[str] = None
    display_name: Optional[str] = None
    email: Optional[str] = None
    is_bot: bool = False
    
    @classmethod
    def from_api_response(cls, user_data: Dict[str, Any]) -> "SlackUser":
        """Create a SlackUser instance from the Slack API response."""
        return cls(
            id=user_data["id"],
            name=user_data["name"],
            real_name=user_data.get("real_name"),
            display_name=user_data.get("profile", {}).get("display_name"),
            email=user_data.get("profile", {}).get("email"),
            is_bot=user_data.get("is_bot", False),
        )


class SlackMessage(BaseModel):
    """Model representing a Slack message."""
    
    channel_id: str
    ts: str  # Timestamp that serves as the message ID
    user_id: Optional[str] = None  # Can be None for system messages
    text: str
    thread_ts: Optional[str] = None  # If part of a thread, the parent message's ts
    reply_count: Optional[int] = None
    files: List[Dict[str, Any]] = Field(default_factory=list)
    reactions: List[Dict[str, Any]] = Field(default_factory=list)
    is_parent: bool = False  # Is this the parent message of a thread
    timestamp: datetime = Field(default_factory=datetime.now)
    
    @classmethod
    def from_api_response(cls, message_data: Dict[str, Any], channel_id: str) -> "SlackMessage":
        """Create a SlackMessage instance from the Slack API response."""
        # Convert Slack timestamp to datetime
        msg_timestamp = datetime.fromtimestamp(float(message_data["ts"]))
        
        # Determine if this is a parent message of a thread
        is_parent = "thread_ts" in message_data and message_data.get("ts") == message_data.get("thread_ts")
        
        return cls(
            channel_id=channel_id,
            ts=message_data["ts"],
            user_id=message_data.get("user"),
            text=message_data["text"],
            thread_ts=message_data.get("thread_ts"),
            reply_count=message_data.get("reply_count"),
            files=message_data.get("files", []),
            reactions=message_data.get("reactions", []),
            is_parent=is_parent,
            timestamp=msg_timestamp,
        )


class SlackThread(BaseModel):
    """Model representing a Slack thread with parent and child messages."""
    
    channel_id: str
    thread_ts: str  # The thread's parent message timestamp
    parent_message: SlackMessage
    replies: List[SlackMessage] = Field(default_factory=list)
    
    @property
    def reply_count(self) -> int:
        """Get the number of replies in the thread."""
        return len(self.replies)
    
    @property
    def participant_ids(self) -> List[str]:
        """Get the list of unique user IDs participating in the thread."""
        all_user_ids = [msg.user_id for msg in [self.parent_message] + self.replies if msg.user_id]
        return list(set(all_user_ids))


class SlackChannel(BaseModel):
    """Model representing a Slack channel."""
    
    id: str
    name: str
    is_private: bool = False
    is_archived: bool = False
    topic: Optional[str] = None
    purpose: Optional[str] = None
    member_count: Optional[int] = None
    
    @classmethod
    def from_api_response(cls, channel_data: Dict[str, Any]) -> "SlackChannel":
        """Create a SlackChannel instance from the Slack API response."""
        return cls(
            id=channel_data["id"],
            name=channel_data["name"],
            is_private=channel_data.get("is_private", False),
            is_archived=channel_data.get("is_archived", False),
            topic=channel_data.get("topic", {}).get("value"),
            purpose=channel_data.get("purpose", {}).get("value"),
            member_count=channel_data.get("num_members"),
        )