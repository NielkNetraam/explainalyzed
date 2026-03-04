"""Add logging to the module."""

import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
