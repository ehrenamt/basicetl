"""Implementation of the TransformOptions class."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TransformOptions:
    """Configuration class for the transformation subservice."""

    in_place: bool = False
    join_type: str = "inner"
    dropna: bool = True
    drop: list =  None
    concatenate: bool = True
