# basic-etl

> A basic tool for ETL processes, intended for local work. Currently in alpha stage.

# Description

basic-etl simplifies ETL processes by abstracting error checking and extracting logic. Simply provide the sources, whether local files or via a remote resource, and specify the transformation and output.

This is aimed for bootstrapping local data analysis projects. It is not meant to be extremely flexible or able to work with large files and data of all types, but moreso a tool for quickly preparing data of different types to be used in local data analysis scripts. It is not designed with cloud environments in mind (such as Databricks or Google Collab), but currently everything _should_ work the same regardless of the environment.

It supports

- Extracting from local ```csv```, ```JSON```, and ```XML``` files.
- Extracting from remote ```csv```, ```JSON```, and ```XML``` files via URL.
- Transforming in-memory data with preset and customized transformations.
- Saving data to disk in ```csv```, ```JSON```, and ```XML``` formats.

I plan to support extracting from SQL databases, to be added later once I have finished all features for the above data types. A full list of planned features is available in [PLANNED.md](PLANNED.md).

# Example Usage

The ```examples``` folder contains an ```example.py``` script, along with sample data.

To run this example, first create a virtual environment.

```bash
python3 -m venv .venv
```

Activate the virtual environment and install the virtual environment based on ```requirements.dev.txt```.

```bash
pip install -r requirements.dev.txt
```

Change the working directory to ```examples```.

```
cd examples
```

Then run the ```examples.py``` script.


```
python example.py
```


# Installation 

Install locally via

```
pip install basic-etl
```


# Tests

Tests are written using ```pytest``` and are located in the ```test``` subdirectory.

Run tests from the project root via 
```
pytest
```

# License
Kindly view [LICENSE.md](LICENSE.md).
