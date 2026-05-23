"""
-------------------------------------------------------------------------------
Copyright (C) 2026 Hussein Adi

This program is free software, and may be used and distributed under the terms
of the included Hippocratic License. You should have received a copy of this
license. If not, see https://github.com/ehrenamt/basicetl/blob/main/LICENSE.md
and the Hippocratic License at https://firstdonoharm.dev/

-------------------------------------------------------------------------------
Filename: load.py

Specifies local outputs.
-------------------------------------------------------------------------------
"""

from basicetl.etl_types import DataType


def load(t_sources: list, l_options: object):
    """
    Saves the sources in the provided list to disk, configuerd by l_options.
    """

    if l_options.save_to_disk:

        index = 0

        try:

            filetype = l_options.filetype

            match filetype:

                case DataType.CSV:

                    for t_source in t_sources:

                        filename = f'{l_options.filename}({index}).csv'
                        t_source.to_csv(filename)
                        index += 1

                case DataType.JSON:

                    for t_source in t_sources:

                        filename = f'{l_options.filename}({index}).json'
                        t_source.to_json(filename)
                        index += 1

                case DataType.XML:

                    for t_source in t_sources:

                        filename = f'{l_options.filename}({index}).xml'
                        t_source.to_xml(filename)
                        index += 1

                case _:
                    pass

        except FileExistsError:
            print("One or more files at specified path already exist.")

        except PermissionError:
            print("Permission denied: Unable to create.")

    return
