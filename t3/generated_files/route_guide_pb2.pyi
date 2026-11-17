from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LogEntry(_message.Message):
    __slots__ = ("index", "term", "command")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    index: int
    term: int
    command: str
    def __init__(self, index: _Optional[int] = ..., term: _Optional[int] = ..., command: _Optional[str] = ...) -> None: ...

class VoteRequest(_message.Message):
    __slots__ = ("term", "candidate_id", "last_log_index", "last_log_term")
    TERM_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int
    def __init__(self, term: _Optional[int] = ..., candidate_id: _Optional[int] = ..., last_log_index: _Optional[int] = ..., last_log_term: _Optional[int] = ...) -> None: ...

class VoteResponse(_message.Message):
    __slots__ = ("term", "vote_granted")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTE_GRANTED_FIELD_NUMBER: _ClassVar[int]
    term: int
    vote_granted: bool
    def __init__(self, term: _Optional[int] = ..., vote_granted: _Optional[bool] = ...) -> None: ...

class FetchRequest(_message.Message):
    __slots__ = ("term", "follower_id", "last_log_index", "last_log_term")
    TERM_FIELD_NUMBER: _ClassVar[int]
    FOLLOWER_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_INDEX_FIELD_NUMBER: _ClassVar[int]
    LAST_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    term: int
    follower_id: int
    last_log_index: int
    last_log_term: int
    def __init__(self, term: _Optional[int] = ..., follower_id: _Optional[int] = ..., last_log_index: _Optional[int] = ..., last_log_term: _Optional[int] = ...) -> None: ...

class FetchResponse(_message.Message):
    __slots__ = ("term", "success", "commit_index", "entries", "leader_id")
    TERM_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    term: int
    success: bool
    commit_index: int
    entries: _containers.RepeatedCompositeFieldContainer[LogEntry]
    leader_id: int
    def __init__(self, term: _Optional[int] = ..., success: _Optional[bool] = ..., commit_index: _Optional[int] = ..., entries: _Optional[_Iterable[_Union[LogEntry, _Mapping]]] = ..., leader_id: _Optional[int] = ...) -> None: ...

class WriteRequest(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: str
    def __init__(self, data: _Optional[str] = ...) -> None: ...

class WriteResponse(_message.Message):
    __slots__ = ("success", "message", "leader_id", "leader_address")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    LEADER_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    message: str
    leader_id: int
    leader_address: str
    def __init__(self, success: _Optional[bool] = ..., message: _Optional[str] = ..., leader_id: _Optional[int] = ..., leader_address: _Optional[str] = ...) -> None: ...

class ReadRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class ReadResponse(_message.Message):
    __slots__ = ("committed_data",)
    COMMITTED_DATA_FIELD_NUMBER: _ClassVar[int]
    committed_data: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, committed_data: _Optional[_Iterable[str]] = ...) -> None: ...
