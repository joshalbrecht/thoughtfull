"""
Tests for the SlackCollector class.
"""
import json
import os
import pickle
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from unittest.mock import MagicMock, patch

import pytest

from thoughtfull.slack.client import SlackClient
from thoughtfull.slack.collector import SlackCollector
from thoughtfull.slack.models import SlackMessage, SlackThread, SlackUser, SlackChannel


@pytest.fixture
def mock_client():
    """Create a mock SlackClient."""
    client = MagicMock(spec=SlackClient)
    
    # Mock channel info response
    client.get_channel_info.return_value = {
        "id": "C12345",
        "name": "general",
        "is_private": False,
        "is_archived": False,
        "topic": {"value": "General discussion"},
        "purpose": {"value": "Company-wide announcements and work-based matters"},
        "num_members": 50,
    }
    
    # Mock user info response
    client.get_user_info.return_value = {
        "id": "U12345",
        "name": "johndoe",
        "real_name": "John Doe",
        "profile": {
            "display_name": "Johnny",
            "email": "john@example.com",
        },
        "is_bot": False,
    }
    
    return client


@pytest.fixture
def sample_messages():
    """Create sample messages for testing."""
    return [
        {
            "ts": "1609459200.123456",  # 2021-01-01 00:00:00
            "text": "This is a parent message",
            "user": "U12345",
            "thread_ts": "1609459200.123456",
            "reply_count": 2,
        },
        {
            "ts": "1609459300.123456",  # About 1 minute later
            "text": "This is a regular message",
            "user": "U67890",
        },
        {
            "ts": "1609459400.123456",  # About 2 minutes later
            "text": "This is another thread parent",
            "user": "U12345",
            "thread_ts": "1609459400.123456",
            "reply_count": 1,
        }
    ]


@pytest.fixture
def sample_thread_messages():
    """Create sample thread messages for testing."""
    return [
        {
            "ts": "1609459200.123456",  # 2021-01-01 00:00:00
            "text": "This is a parent message",
            "user": "U12345",
            "thread_ts": "1609459200.123456",
            "reply_count": 2,
        },
        {
            "ts": "1609459250.123456",  # 50 seconds later
            "text": "This is a reply",
            "user": "U67890",
            "thread_ts": "1609459200.123456",
        },
        {
            "ts": "1609459350.123456",  # 150 seconds later
            "text": "This is another reply",
            "user": "U12345",
            "thread_ts": "1609459200.123456",
        }
    ]


@pytest.fixture
def collector(mock_client):
    """Create a SlackCollector with a mock client."""
    return SlackCollector(client=mock_client)


@pytest.fixture
def sample_threads(collector):
    """Create sample threads for testing."""
    # Create a parent message
    parent = SlackMessage(
        channel_id="C12345",
        ts="1609459200.123456",  # 2021-01-01 00:00:00
        user_id="U12345",
        text="This is a parent message with keywords apple banana",
        thread_ts="1609459200.123456",
        reply_count=2,
        is_parent=True,
        timestamp=datetime(2021, 1, 1, 0, 0, 0),
    )
    
    # Create replies
    replies = [
        SlackMessage(
            channel_id="C12345",
            ts="1609459250.123456",  # 50 seconds later
            user_id="U67890",
            text="This is a reply with keyword cherry",
            thread_ts="1609459200.123456",
            timestamp=datetime(2021, 1, 1, 0, 0, 50),
        ),
        SlackMessage(
            channel_id="C12345",
            ts="1609459350.123456",  # 150 seconds later
            user_id="U12345",
            text="This is another reply with keyword banana",
            thread_ts="1609459200.123456",
            timestamp=datetime(2021, 1, 1, 0, 2, 30),
        )
    ]
    
    # Create thread
    thread1 = SlackThread(
        channel_id="C12345",
        thread_ts="1609459200.123456",
        parent_message=parent,
        replies=replies,
    )
    
    # Create another thread with different users
    parent2 = SlackMessage(
        channel_id="C12345",
        ts="1609459400.123456",  # 2021-01-01 00:03:20
        user_id="U67890",
        text="This is another parent message with keywords cherry date",
        thread_ts="1609459400.123456",
        reply_count=1,
        is_parent=True,
        timestamp=datetime(2021, 1, 1, 0, 3, 20),
    )
    
    replies2 = [
        SlackMessage(
            channel_id="C12345",
            ts="1609459450.123456",  # 50 seconds later
            user_id="U54321",
            text="This is a reply to the second thread with keyword elderberry",
            thread_ts="1609459400.123456",
            timestamp=datetime(2021, 1, 1, 0, 4, 10),
        )
    ]
    
    thread2 = SlackThread(
        channel_id="C12345",
        thread_ts="1609459400.123456",
        parent_message=parent2,
        replies=replies2,
    )
    
    return [thread1, thread2]


def test_init(mock_client):
    """Test initialization of SlackCollector."""
    collector = SlackCollector(client=mock_client)
    assert collector.client == mock_client
    assert collector._users_cache == {}
    assert collector._channels_cache == {}


def test_get_user(collector, mock_client):
    """Test getting a user with caching."""
    # First call should hit the API
    user = collector.get_user("U12345")
    mock_client.get_user_info.assert_called_once_with("U12345")
    
    assert user.id == "U12345"
    assert user.name == "johndoe"
    assert user.real_name == "John Doe"
    assert user.display_name == "Johnny"
    assert user.email == "john@example.com"
    assert user.is_bot == False
    
    # Reset the mock to check if second call uses cache
    mock_client.reset_mock()
    
    # Second call should use the cache
    user2 = collector.get_user("U12345")
    mock_client.get_user_info.assert_not_called()
    
    # User objects should be the same (from cache)
    assert user == user2


def test_get_channel(collector, mock_client):
    """Test getting a channel with caching."""
    # First call should hit the API
    channel = collector.get_channel("C12345")
    mock_client.get_channel_info.assert_called_once_with("C12345")
    
    assert channel.id == "C12345"
    assert channel.name == "general"
    assert channel.is_private == False
    assert channel.is_archived == False
    assert channel.topic == "General discussion"
    assert channel.purpose == "Company-wide announcements and work-based matters"
    assert channel.member_count == 50
    
    # Reset the mock to check if second call uses cache
    mock_client.reset_mock()
    
    # Second call should use the cache
    channel2 = collector.get_channel("C12345")
    mock_client.get_channel_info.assert_not_called()
    
    # Channel objects should be the same (from cache)
    assert channel == channel2


def test_collect_threads_from_channel(collector, mock_client, sample_messages, sample_thread_messages):
    """Test collecting threads from a channel."""
    # Mock the channel messages response
    mock_client.get_channel_messages.return_value = sample_messages
    
    # Mock the thread messages response
    mock_client.get_thread_messages.return_value = sample_thread_messages
    
    # Call the method
    threads = collector.collect_threads_from_channel("C12345", min_thread_length=1)
    
    # Assert that channel messages were fetched
    mock_client.get_channel_messages.assert_called_once_with(
        channel_id="C12345",
        limit=100,
        oldest=None,
        latest=None,
    )
    
    # Assert that thread messages were fetched for parent messages
    assert mock_client.get_thread_messages.call_count == 2
    mock_client.get_thread_messages.assert_any_call("C12345", "1609459200.123456")
    mock_client.get_thread_messages.assert_any_call("C12345", "1609459400.123456")
    
    # Assert that threads were created correctly
    assert len(threads) == 2
    assert threads[0].thread_ts == "1609459200.123456"
    assert len(threads[0].replies) == 2
    assert threads[1].thread_ts == "1609459400.123456"


def test_collect_threads_from_channels(collector):
    """Test collecting threads from multiple channels."""
    # Mock the collect_threads_from_channel method
    with patch.object(collector, 'collect_threads_from_channel') as mock_collect:
        # Set up the return values for each channel
        mock_collect.side_effect = [
            [MagicMock(), MagicMock()],  # 2 threads for first channel
            [MagicMock()],  # 1 thread for second channel
            Exception("Channel error"),  # Error for third channel
        ]
        
        # Call the method
        result = collector.collect_threads_from_channels(
            ["C12345", "C67890", "CERROR"],
            limit_per_channel=50,
            min_thread_length=2,
        )
        
        # Check calls
        assert mock_collect.call_count == 3
        mock_collect.assert_any_call(
            channel_id="C12345",
            limit=50,
            oldest=None,
            latest=None,
            min_thread_length=2,
        )
        
        # Check results
        assert len(result) == 3
        assert len(result["C12345"]) == 2
        assert len(result["C67890"]) == 1
        assert len(result["CERROR"]) == 0  # Error case should return empty list


def test_filter_threads_by_date(collector, sample_threads):
    """Test filtering threads by date."""
    # Define date ranges
    start_date = datetime(2021, 1, 1, 0, 1, 0)  # After first thread parent
    end_date = datetime(2021, 1, 1, 0, 4, 0)    # Before second thread's reply
    
    # Test with only start_date
    filtered = collector.filter_threads_by_date(sample_threads, start_date=start_date)
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459400.123456"  # Second thread
    
    # Test with only end_date
    filtered = collector.filter_threads_by_date(sample_threads, end_date=end_date)
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459200.123456"  # First thread
    
    # Test with both dates
    filtered = collector.filter_threads_by_date(
        sample_threads,
        start_date=datetime(2020, 12, 1),
        end_date=datetime(2021, 1, 10),
    )
    assert len(filtered) == 2  # Both threads
    
    # Test with dates excluding all threads
    filtered = collector.filter_threads_by_date(
        sample_threads,
        start_date=datetime(2021, 1, 2),
    )
    assert len(filtered) == 0


def test_filter_threads_by_keywords(collector, sample_threads):
    """Test filtering threads by keywords."""
    # Test with a single keyword that exists in both threads
    filtered = collector.filter_threads_by_keywords(sample_threads, ["banana"])
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459200.123456"  # First thread
    
    # Test with multiple keywords, any match
    filtered = collector.filter_threads_by_keywords(
        sample_threads, 
        ["banana", "elderberry"],
        match_all=False,
    )
    assert len(filtered) == 2  # Both threads
    
    # Test with multiple keywords, all must match
    filtered = collector.filter_threads_by_keywords(
        sample_threads, 
        ["apple", "banana"],
        match_all=True,
    )
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459200.123456"  # First thread
    
    # Test case sensitivity
    filtered = collector.filter_threads_by_keywords(
        sample_threads, 
        ["APPLE"],
        case_sensitive=True,
    )
    assert len(filtered) == 0  # No match with case sensitivity
    
    filtered = collector.filter_threads_by_keywords(
        sample_threads, 
        ["APPLE"],
        case_sensitive=False,
    )
    assert len(filtered) == 1  # Match with case insensitivity
    
    # Test with non-existent keyword
    filtered = collector.filter_threads_by_keywords(sample_threads, ["nonexistent"])
    assert len(filtered) == 0


def test_filter_threads_by_regex(collector, sample_threads):
    """Test filtering threads by regex pattern."""
    import re
    
    # Test basic pattern
    filtered = collector.filter_threads_by_regex(sample_threads, r"apple.*banana")
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459200.123456"  # First thread
    
    # Test with flags
    filtered = collector.filter_threads_by_regex(sample_threads, r"APPLE", flags=re.IGNORECASE)
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459200.123456"  # First thread
    
    # Test with no matches
    filtered = collector.filter_threads_by_regex(sample_threads, r"nonexistent\d+")
    assert len(filtered) == 0


def test_filter_threads_by_users(collector, sample_threads):
    """Test filtering threads by participating users."""
    # Test with single user
    filtered = collector.filter_threads_by_users(sample_threads, ["U54321"])
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459400.123456"  # Second thread
    
    # Test with multiple users, any match
    filtered = collector.filter_threads_by_users(
        sample_threads, 
        ["U12345", "U54321"],
        require_all=False,
    )
    assert len(filtered) == 2  # Both threads
    
    # Test with multiple users, all must match
    filtered = collector.filter_threads_by_users(
        sample_threads, 
        ["U12345", "U67890"],
        require_all=True,
    )
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459200.123456"  # First thread
    
    # Test with non-existent user
    filtered = collector.filter_threads_by_users(sample_threads, ["UNONEXISTENT"])
    assert len(filtered) == 0


def test_filter_threads_by_custom_function(collector, sample_threads):
    """Test filtering threads by custom function."""
    # Define custom filter functions
    def has_more_than_one_reply(thread):
        return len(thread.replies) > 1
    
    def has_specific_user(thread):
        return "U54321" in thread.participant_ids
    
    # Test first filter
    filtered = collector.filter_threads_by_custom_function(sample_threads, has_more_than_one_reply)
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459200.123456"  # First thread
    
    # Test second filter
    filtered = collector.filter_threads_by_custom_function(sample_threads, has_specific_user)
    assert len(filtered) == 1
    assert filtered[0].thread_ts == "1609459400.123456"  # Second thread


def test_save_load_threads_pickle(collector, sample_threads, tmp_path):
    """Test saving and loading threads to/from pickle."""
    # Test with file path
    pickle_path = tmp_path / "threads.pickle"
    collector.save_threads_pickle(sample_threads, pickle_path)
    
    # Verify file exists
    assert pickle_path.exists()
    
    # Load the threads
    loaded_threads = SlackCollector.load_threads_pickle(pickle_path)
    
    # Verify loaded data
    assert len(loaded_threads) == len(sample_threads)
    assert loaded_threads[0].thread_ts == sample_threads[0].thread_ts
    assert loaded_threads[1].thread_ts == sample_threads[1].thread_ts
    
    # Test with file-like object
    file_obj = BytesIO()
    collector.save_threads_pickle(sample_threads, file_obj)
    
    # Reset file position for reading
    file_obj.seek(0)
    
    # Load the threads
    loaded_threads = SlackCollector.load_threads_pickle(file_obj)
    
    # Verify loaded data
    assert len(loaded_threads) == len(sample_threads)
    assert loaded_threads[0].thread_ts == sample_threads[0].thread_ts


def test_save_threads_json(collector, sample_threads, tmp_path):
    """Test saving threads to JSON."""
    # Test with file path
    json_path = tmp_path / "threads.json"
    collector.save_threads_json(sample_threads, json_path, pretty=True)
    
    # Verify file exists
    assert json_path.exists()
    
    # Read the JSON file and verify basic structure
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    assert len(data) == len(sample_threads)
    assert data[0]["thread_ts"] == sample_threads[0].thread_ts
    
    # Test with file-like object
    file_obj = StringIO()
    collector.save_threads_json(sample_threads, file_obj)
    
    # Get the JSON content
    json_content = file_obj.getvalue()
    
    # Verify basic structure
    data = json.loads(json_content)
    assert len(data) == len(sample_threads)


def test_export_threads_text(collector, sample_threads, tmp_path):
    """Test exporting threads to text."""
    # Mock the get_user and get_channel methods
    collector.get_user = MagicMock(return_value=SlackUser(
        id="U12345",
        name="johndoe",
        real_name="John Doe",
        display_name="Johnny",
    ))
    
    collector.get_channel = MagicMock(return_value=SlackChannel(
        id="C12345",
        name="general",
    ))
    
    # Test with file path
    text_path = tmp_path / "threads.txt"
    collector.export_threads_text(sample_threads, text_path)
    
    # Verify file exists
    assert text_path.exists()
    
    # Read the text file and check content
    with open(text_path, 'r') as f:
        content = f.read()
    
    # Verify that key elements are present
    assert "Thread in #general" in content
    assert "John Doe (@johndoe)" in content
    assert "This is a parent message with keywords apple banana" in content
    
    # Test with file-like object
    file_obj = StringIO()
    collector.export_threads_text(sample_threads, file_obj, include_user_info=False)
    
    # Get the text content
    text_content = file_obj.getvalue()
    
    # Verify format without user info
    assert "Thread in #general" in text_content
    assert "User: U12345" in text_content  # Should show user ID instead of name