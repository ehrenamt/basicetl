"""Unit tests and mock tests for submodule.transform."""

# pylint: disable=missing-docstring

import pandas as pd
import pytest
from basicetl.submodules import transform as tf


# setting up mock data for tests

accounts_a1 = {
    "id": ["000", "001", "002"],
    "uname": ["Elsy", " Kaveri",  "Shabani"],
}

accounts_a2 = {
    "id": ["003", "004", "005"],
    "uname": ["Yishai", "Mukta",  "Leto"],
}

accounts_b1 = {
    "id": ["000", "001", "002"],
    "uname": ["Elsy", " Kaveri",  "Shabani"],
    "email": ["example@example.com", "example@example.com",  "example@example.com"],
}

df_a1 = pd.DataFrame(accounts_a1)
df_a2 = pd.DataFrame(accounts_a2)
df_b1 = pd.DataFrame(accounts_b1)


# tests


@pytest.mark.parametrize("input_df_1, input_df_2, expected_bool", [
    (df_a1, df_a2, True),
    (df_a1, df_b1, False),
])

def test_check_schema_match(input_df_1, input_df_2, expected_bool):
    assert tf.check_schema_match(input_df_1, input_df_2) == expected_bool
