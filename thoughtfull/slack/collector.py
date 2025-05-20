"""
Collector for aggregating and processing Slack messages.
"""
import json
import os
import pickle
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Callable, Union, BinaryIO, TextIO

from loguru import logger

from thoughtfull.slack.client import SlackClient
from thoughtfull.slack.models import SlackMessage, SlackThread, SlackUser, SlackChannel


class SlackCollector:
    """Collector for aggregating and processing Slack messages from Slack workspaces."""

    def __init__(self, client: Optional[SlackClient] = None, token: Optional[str] = None):
        """
        Initialize the Slack collector.

        Args:
            client: Slack client instance. If not provided, a new one will be created.
            token: Slack API token. Only used if client is not provided.
        """
        self.client = client or SlackClient(token=token)
        self._users_cache: Dict[str, SlackUser] = {}
        self._channels_cache: Dict[str, SlackChannel] = {}
        logger.debug("Initialized Slack collector")

    def get_user(self, user_id: str) -> SlackUser:
        """
        Get information about a user, with caching.

        Args:
            user_id: ID of the user

        Returns:
            SlackUser model
        """
        if user_id not in self._users_cache:
            user_data = self.client.get_user_info(user_id)
            self._users_cache[user_id] = SlackUser.from_api_response(user_data)
        return self._users_cache[user_id]

    def get_channel(self, channel_id: str) -> SlackChannel:
        """
        Get information about a channel, with caching.

        Args:
            channel_id: ID of the channel

        Returns:
            SlackChannel model
        """
        if channel_id not in self._channels_cache:
            channel_data = self.client.get_channel_info(channel_id)
            self._channels_cache[channel_id] = SlackChannel.from_api_response(channel_data)
        return self._channels_cache[channel_id]

    def collect_threads_from_channel(
        self,
        channel_id: str,
        limit: int = 100,
        oldest: Optional[str] = None,
        latest: Optional[str] = None,
        min_thread_length: int = 1,
    ) -> List[SlackThread]:
        """
        Collect threads from a channel.

        Args:
            channel_id: ID of the channel to fetch messages from
            limit: Maximum number of parent messages to check for threads
            oldest: Start of time range to include messages (timestamp)
            latest: End of time range to include messages (timestamp)
            min_thread_length: Minimum number of replies to consider a thread worth collecting

        Returns:
            List of SlackThread objects
        """
        logger.info(f"Collecting threads from channel {channel_id}")
        
        # Get channel info for validation
        channel = self.get_channel(channel_id)
        if channel.is_archived:
            logger.warning(f"Channel {channel.name} ({channel_id}) is archived")
        
        # Get messages from the channel
        messages = self.client.get_channel_messages(
            channel_id=channel_id,
            limit=limit,
            oldest=oldest,
            latest=latest,
        )
        
        # Find messages that are part of threads (either parent or replies)
        threads: List[SlackThread] = []
        thread_ts_set: Set[str] = set()
        
        # First pass: find all unique thread_ts values (parent messages)
        for message in messages:
            # Check if this message is a thread parent with replies
            if message.get("thread_ts") and message.get("thread_ts") == message.get("ts"):
                if message.get("reply_count", 0) >= min_thread_length:
                    thread_ts_set.add(message["ts"])
        
        # Second pass: collect thread messages for each parent
        for thread_ts in thread_ts_set:
            try:
                thread_messages = self.client.get_thread_messages(channel_id, thread_ts)
                
                if not thread_messages:
                    continue
                
                # First message is the parent
                parent_data = thread_messages[0]
                parent_message = SlackMessage.from_api_response(parent_data, channel_id)
                
                # Rest are replies
                replies = [
                    SlackMessage.from_api_response(msg, channel_id)
                    for msg in thread_messages[1:]
                ]
                
                thread = SlackThread(
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                    parent_message=parent_message,
                    replies=replies,
                )
                
                threads.append(thread)
                logger.debug(f"Collected thread with {len(replies)} replies: {thread_ts}")
            
            except Exception as e:
                logger.error(f"Error collecting thread {thread_ts}: {e}")
        
        logger.info(f"Collected {len(threads)} threads from channel {channel_id}")
        return threads

    def collect_threads_from_channels(
        self,
        channel_ids: List[str],
        limit_per_channel: int = 100,
        oldest: Optional[str] = None,
        latest: Optional[str] = None,
        min_thread_length: int = 1,
    ) -> Dict[str, List[SlackThread]]:
        """
        Collect threads from multiple channels.

        Args:
            channel_ids: List of channel IDs to fetch messages from
            limit_per_channel: Maximum number of parent messages to check for threads in each channel
            oldest: Start of time range to include messages (timestamp)
            latest: End of time range to include messages (timestamp)
            min_thread_length: Minimum number of replies to consider a thread worth collecting

        Returns:
            Dictionary mapping channel IDs to lists of SlackThread objects
        """
        result: Dict[str, List[SlackThread]] = {}
        
        for channel_id in channel_ids:
            try:
                threads = self.collect_threads_from_channel(
                    channel_id=channel_id,
                    limit=limit_per_channel,
                    oldest=oldest,
                    latest=latest,
                    min_thread_length=min_thread_length,
                )
                result[channel_id] = threads
            except Exception as e:
                logger.error(f"Error collecting threads from channel {channel_id}: {e}")
                result[channel_id] = []
        
        return result
        
    def filter_threads_by_date(
        self, 
        threads: List[SlackThread], 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[SlackThread]:
        """
        Filter threads by date range.
        
        Args:
            threads: List of threads to filter
            start_date: Include threads with messages after this date
            end_date: Include threads with messages before this date
            
        Returns:
            Filtered list of threads
        """
        if not start_date and not end_date:
            return threads
            
        filtered_threads = []
        
        for thread in threads:
            # Check if the parent message is within the date range
            parent_time = thread.parent_message.timestamp
            
            if start_date and parent_time < start_date:
                continue
                
            if end_date and parent_time > end_date:
                continue
                
            filtered_threads.append(thread)
            
        logger.debug(f"Filtered threads by date: {len(filtered_threads)}/{len(threads)} threads remaining")
        return filtered_threads
        
    def filter_threads_by_keywords(
        self,
        threads: List[SlackThread],
        keywords: List[str],
        match_all: bool = False,
        case_sensitive: bool = False
    ) -> List[SlackThread]:
        """
        Filter threads by keywords in message text.
        
        Args:
            threads: List of threads to filter
            keywords: List of keywords to search for
            match_all: If True, all keywords must be present; if False, any keyword is sufficient
            case_sensitive: Whether to perform case-sensitive matching
            
        Returns:
            Filtered list of threads
        """
        if not keywords:
            return threads
            
        filtered_threads = []
        
        for thread in threads:
            # Combine all message texts in the thread
            all_messages = [thread.parent_message] + thread.replies
            combined_text = " ".join([msg.text for msg in all_messages])
            
            if not case_sensitive:
                combined_text = combined_text.lower()
                search_keywords = [k.lower() for k in keywords]
            else:
                search_keywords = keywords
            
            # Check for keyword matches
            if match_all:
                if all(keyword in combined_text for keyword in search_keywords):
                    filtered_threads.append(thread)
            else:
                if any(keyword in combined_text for keyword in search_keywords):
                    filtered_threads.append(thread)
        
        logger.debug(f"Filtered threads by keywords: {len(filtered_threads)}/{len(threads)} threads remaining")
        return filtered_threads
        
    def filter_threads_by_regex(
        self,
        threads: List[SlackThread],
        pattern: str,
        flags: int = 0
    ) -> List[SlackThread]:
        """
        Filter threads by regex pattern in message text.
        
        Args:
            threads: List of threads to filter
            pattern: Regular expression pattern to match
            flags: Regex flags (e.g., re.IGNORECASE)
            
        Returns:
            Filtered list of threads
        """
        filtered_threads = []
        compiled_pattern = re.compile(pattern, flags)
        
        for thread in threads:
            # Combine all message texts in the thread
            all_messages = [thread.parent_message] + thread.replies
            combined_text = " ".join([msg.text for msg in all_messages])
            
            # Check for regex match
            if compiled_pattern.search(combined_text):
                filtered_threads.append(thread)
        
        logger.debug(f"Filtered threads by regex: {len(filtered_threads)}/{len(threads)} threads remaining")
        return filtered_threads
        
    def filter_threads_by_users(
        self,
        threads: List[SlackThread],
        user_ids: List[str],
        require_all: bool = False
    ) -> List[SlackThread]:
        """
        Filter threads by participating users.
        
        Args:
            threads: List of threads to filter
            user_ids: List of user IDs to filter by
            require_all: If True, all users must participate; if False, any user is sufficient
            
        Returns:
            Filtered list of threads
        """
        if not user_ids:
            return threads
            
        filtered_threads = []
        
        for thread in threads:
            thread_participants = thread.participant_ids
            
            if require_all:
                if all(user_id in thread_participants for user_id in user_ids):
                    filtered_threads.append(thread)
            else:
                if any(user_id in thread_participants for user_id in user_ids):
                    filtered_threads.append(thread)
        
        logger.debug(f"Filtered threads by users: {len(filtered_threads)}/{len(threads)} threads remaining")
        return filtered_threads
        
    def filter_threads_by_custom_function(
        self,
        threads: List[SlackThread],
        filter_function: Callable[[SlackThread], bool]
    ) -> List[SlackThread]:
        """
        Filter threads using a custom function.
        
        Args:
            threads: List of threads to filter
            filter_function: Function that takes a SlackThread and returns a boolean
            
        Returns:
            Filtered list of threads
        """
        filtered_threads = [thread for thread in threads if filter_function(thread)]
        logger.debug(f"Filtered threads by custom function: {len(filtered_threads)}/{len(threads)} threads remaining")
        return filtered_threads
        
    def save_threads_pickle(
        self,
        threads: List[SlackThread],
        file_path: Union[str, Path, BinaryIO],
        protocol: int = pickle.HIGHEST_PROTOCOL
    ) -> None:
        """
        Save threads to a pickle file.
        
        Args:
            threads: List of threads to save
            file_path: Path to save the pickle file, or a file-like object
            protocol: Pickle protocol version
        """
        logger.info(f"Saving {len(threads)} threads to pickle")
        
        if isinstance(file_path, (str, Path)):
            with open(file_path, 'wb') as f:
                pickle.dump(threads, f, protocol=protocol)
            logger.info(f"Saved threads to {file_path}")
        else:
            # Assume file_path is a file-like object
            pickle.dump(threads, file_path, protocol=protocol)
            logger.info("Saved threads to file object")
    
    @staticmethod
    def load_threads_pickle(
        file_path: Union[str, Path, BinaryIO]
    ) -> List[SlackThread]:
        """
        Load threads from a pickle file.
        
        Args:
            file_path: Path to the pickle file, or a file-like object
            
        Returns:
            List of SlackThread objects
        """
        if isinstance(file_path, (str, Path)):
            with open(file_path, 'rb') as f:
                threads = pickle.load(f)
            logger.info(f"Loaded threads from {file_path}")
        else:
            # Assume file_path is a file-like object
            threads = pickle.load(file_path)
            logger.info("Loaded threads from file object")
            
        return threads
    
    def save_threads_json(
        self,
        threads: List[SlackThread],
        file_path: Union[str, Path, TextIO],
        pretty: bool = False
    ) -> None:
        """
        Save threads to a JSON file.
        
        Args:
            threads: List of threads to save
            file_path: Path to save the JSON file, or a file-like object
            pretty: Whether to pretty-print the JSON output
        """
        logger.info(f"Saving {len(threads)} threads to JSON")
        
        # Convert threads to dictionaries (compatible with JSON serialization)
        threads_data = [thread.dict() for thread in threads]
        
        # Handle datetime objects (convert to ISO format string)
        def serialize_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        indent = 4 if pretty else None
        
        if isinstance(file_path, (str, Path)):
            with open(file_path, 'w') as f:
                json.dump(threads_data, f, default=serialize_datetime, indent=indent)
            logger.info(f"Saved threads to {file_path}")
        else:
            # Assume file_path is a file-like object
            json.dump(threads_data, file_path, default=serialize_datetime, indent=indent)
            logger.info("Saved threads to file object")
    
    def export_threads_text(
        self,
        threads: List[SlackThread],
        file_path: Union[str, Path, TextIO],
        include_user_info: bool = True,
        separator: str = "="*80,
    ) -> None:
        """
        Export threads to a human-readable text file.
        
        Args:
            threads: List of threads to export
            file_path: Path to save the text file, or a file-like object
            include_user_info: Whether to include user information (name, real name)
            separator: String to use as separator between threads
        """
        logger.info(f"Exporting {len(threads)} threads to text")
        
        if isinstance(file_path, (str, Path)):
            f = open(file_path, 'w')
            close_file = True
        else:
            # Assume file_path is a file-like object
            f = file_path
            close_file = False
        
        try:
            for i, thread in enumerate(threads):
                if i > 0:
                    f.write(f"\n{separator}\n\n")
                
                # Get channel info
                try:
                    channel = self.get_channel(thread.channel_id)
                    channel_name = channel.name
                except Exception:
                    channel_name = f"Channel: {thread.channel_id}"
                
                # Write thread header
                f.write(f"Thread in #{channel_name} (Timestamp: {thread.thread_ts})\n\n")
                
                # Write parent message
                parent = thread.parent_message
                parent_time = parent.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                
                if include_user_info and parent.user_id:
                    try:
                        user = self.get_user(parent.user_id)
                        user_display = f"{user.real_name or user.display_name or user.name} (@{user.name})"
                    except Exception:
                        user_display = f"User: {parent.user_id}"
                else:
                    user_display = f"User: {parent.user_id}" if parent.user_id else "System Message"
                
                f.write(f"{parent_time} - {user_display}:\n{parent.text}\n\n")
                
                # Write replies
                for reply in thread.replies:
                    reply_time = reply.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    
                    if include_user_info and reply.user_id:
                        try:
                            user = self.get_user(reply.user_id)
                            user_display = f"{user.real_name or user.display_name or user.name} (@{user.name})"
                        except Exception:
                            user_display = f"User: {reply.user_id}"
                    else:
                        user_display = f"User: {reply.user_id}" if reply.user_id else "System Message"
                    
                    f.write(f"{reply_time} - {user_display}:\n{reply.text}\n\n")
            
            logger.info(f"Exported {len(threads)} threads to text")
            
        finally:
            if close_file:
                f.close()