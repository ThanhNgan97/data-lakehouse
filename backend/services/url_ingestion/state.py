from datetime import datetime, timezone


ACTIVE_FILE_STATES = {"QUEUED", "DOWNLOADING", "PROCESSING"}
SUCCESS_FILE_STATES = {"COMPLETED", "DUPLICATE"}
FAILURE_FILE_STATES = {"FAILED", "UNSUPPORTED"}


def derive_job_status(states: list[str], cancel_requested: bool = False) -> str:
    if not states:
        return "PENDING"
    if any(state in ACTIVE_FILE_STATES for state in states):
        return "PROCESSING"
    if cancel_requested and all(state == "CANCELLED" for state in states):
        return "CANCELLED"
    success = any(state in SUCCESS_FILE_STATES for state in states)
    failure = any(state in FAILURE_FILE_STATES for state in states)
    if all(state in SUCCESS_FILE_STATES for state in states):
        return "COMPLETED"
    if success and failure:
        return "PARTIAL_SUCCESS"
    if failure and not success:
        return "FAILED"
    if all(state == "CANCELLED" for state in states):
        return "CANCELLED"
    return "FAILED"


def utcnow():
    return datetime.now(timezone.utc)
