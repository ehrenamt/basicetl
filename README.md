# basicetl

> [WORK IN PROGRESS] A basic tool for ETL processes, mostly for local work.

# Description

basicetl simplifies ETL processes by abstracting error checking and extracting logic. Simply provide the sources, whether local files or via a remote resource, and specify the transformation and output.

Useful for bootstrapping local data analysis and ML projects.

Currently does not work with zipped files, but it should be a simple fix. I am working on SQL support as well, but without security features.

It supports

- Extracting from local ```csv```, ```JSON```, ```XML``` files.
- Extracting from remote ```csv```, ```JSON```, ```XML``` files via URL.

I plan to support extracting from SQL databases, to be added later once I have finished all features for the above data types.

# Examples

WIP

# Installation and Packages

WIP

# Tests

[WORK IN PROGRESS] Tests are written using ```pytest``` and are located in the ```test``` subdirectory.
