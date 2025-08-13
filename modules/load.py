# load.py - specifies outputs, either locally or to a data warehouse

import os

import pandas as pd


def save_local(sources: list, normalize_filetype=True):
    LOCAL_DATA_PATH = "transformed_data"
    try:
        os.makedirs(LOCAL_DATA_PATH)

    except FileExistsError:
        print(f"One or more directories in '{LOCAL_DATA_PATH}' already exist.")

    except PermissionError:
        print(f"Permission denied: Unable to create '{LOCAL_DATA_PATH}'.")


    for i, source in enumerate(sources):
        if normalize_filetype:
            filename = os.path.join(LOCAL_DATA_PATH,  f'data_{i+1}.csv')
            source.to_csv(filename, index=False)
