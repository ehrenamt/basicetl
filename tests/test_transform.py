"""Unit tests and mock tests for submodule.transform."""

# pylint: disable=missing-docstring

import pandas as pd
import pytest
from basicetl.submodules import transform as tf
from basicetl.etl_options import TransformOptions

# -----------------------------------------------------------------------------
# setting up mock data for tests
# -----------------------------------------------------------------------------


accounts_a1 = {
    "id": ["000", "001", "002"],
    "uname": ["Elsy", " Kaveri",  "Shabani"],
}

accounts_a2 = {
    "id": ["003", "004", "005"],
    "uname": ["Yishai", "Mukta",  "Leto"],
}

accounts_a_concatenated = {
    "id": ["000", "001", "002", "003", "004", "005"],
    "uname": ["Elsy", " Kaveri",  "Shabani", "Yishai", "Mukta",  "Leto"],
}

# accounts_b1 = {
#     "id": ["000", "001", "002"],
#     "uname": ["Elsy", " Kaveri",  "Shabani"],
#     "email": ["example@example.com", "example@example.com",  "example@example.com"],
# }

accounts_b1 = {
    "id": ["000", "001", "002"],
    "uname": ["Elsy", " Kaveri", None],
    "email": ["example@example.com", "example@example.com",  "example@example.com"],
}

accounts_b1_na_removed = {
    "id": ["000", "001"],
    "uname": ["Elsy", " Kaveri"],
    "email": ["example@example.com", "example@example.com"],
}


df_a1 = pd.DataFrame(accounts_a1)
df_a2 = pd.DataFrame(accounts_a2)
df_a_concatenated = pd.DataFrame(accounts_a_concatenated)
df_b1 = pd.DataFrame(accounts_b1)
df_b1_na_removed = pd.DataFrame(accounts_b1_na_removed)


# -----------------------------------------------------------------------------
# Test definitions
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("input_df_1, input_df_2, expected_bool", [
    (df_a1, df_a2, True),
    (df_a1, df_b1, False),
])
def test_check_schema_match(input_df_1, input_df_2, expected_bool):
    assert tf.check_schema_match(input_df_1, input_df_2) == expected_bool

@pytest.mark.parametrize("input_df_1, input_df_2, expected_bool", [
    (df_b1, df_b1_na_removed, True),
])
def test_check_configured_dropna(input_df_1, input_df_2, expected_bool):
    t_options = TransformOptions()

    transformed = tf.configured_transform(input_df_1, t_options)
    assert transformed.equals(input_df_2) == expected_bool

@pytest.mark.parametrize("input_sources, expected_df, expected_bool", [
    ([df_a1, df_a2], df_a_concatenated, True),
])
def test_concatenate_sources(input_sources, expected_df, expected_bool):
    result = tf.concatenate_sources(input_sources)[0]
    assert result.equals(expected_df) == expected_bool
