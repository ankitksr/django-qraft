"""
Logging filter that stamps every record with the executing attempt's ids.

Attach it to a handler and the host's formatter can print or ship
`qraft_task_id`, `qraft_attempt_id`, `qraft_attempt_number`, `qraft_graph_id`,
`qraft_node_key`, `qraft_generation`, `qraft_subject_type` and `qraft_subject_id`.
Outside a task the
attributes are present and empty, so a format string that names them never
raises.
"""

import logging

from qraft.context import current_context

ATTRIBUTES = (
    "task_id",
    "attempt_id",
    "attempt_number",
    "graph_id",
    "node_key",
    "generation",
    "subject_type",
    "subject_id",
)


class QraftContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        context = current_context()
        for name in ATTRIBUTES:
            value = context.get(name)
            setattr(record, f"qraft_{name}", "" if value is None else value)
        return True
