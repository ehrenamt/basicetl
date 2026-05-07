# basicetl

> A basic tool for ETL processes, intended for local work.

_Version française de ce texte est disponible à [README.fr.md](README.fr.md)_

# Description

basicetl simplifies ETL processes by abstracting error checking and extracting logic. Simply provide the sources, whether local files or via a remote resource, and specify the transformation and output.

Useful for bootstrapping local data analysis and ML projects. It is not meant to be extremely flexible or able to work with large files and data of all types, but moreso a tool for quickly preparing data of different types to be used in local data analysis scripts. It is not designed with cloud environments in mind (such as Databricks or Google Collab), but currently everything _should_ work the same regardless of the environment.

It supports

- Extracting from local ```csv```, ```JSON```, ```XML``` files.
- Extracting from remote ```csv```, ```JSON```, ```XML``` files via URL.

I plan to support extracting from SQL databases, to be added later once I have finished all features for the above data types. A full list of planned features is available in [PLANNED.md](PLANNED.md).

# Example Usage

To be completed.

# Installation and Packages

See ```requirements.txt``` for dependencies.

# Tests

Tests are written using ```pytest``` and are located in the ```test``` subdirectory.

Run tests from the project root via 
```
pytest
```

# License
Kindly view [LICENSE.md](LICENSE.md).
