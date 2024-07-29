# pylint: disable=C0103
"""Custom Types.

This module contains custom type definitions used throughout the project.
"""

from typing import List, Dict

Index = str # e.g. '2024-26'
IndexList = List[Index] # e.g. ['2024-26']

Result = Dict
ResultList = List[Dict]