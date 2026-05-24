# Planned Features (Tentative)

The list herein is a tentative plan of features that we intend to add.
This list is tentative as some of these features may not fall within the scope
of this project. We aim to keep basic-etl fairly lightweight, even if it does
mean lack of support or features regarding certain filetypes or data types.

## Extraction / Extract Options Planned Features
- Batch extraction of large files (on disk)
- Batch querying of large sql files
- Support for pyarrow.table
- Saving extracted results to disk (if files take up too much memory)

## Transformation / Transform Options Planned Features 
In addition to basic transformations such as merging, joining, and cleaning,
we'll add the functionality to pass in functions that perform normalization.
Likely needing a control object or something similar, and function templates,
in order to quickly add and remove transformation steps.
This of course depends on the schema, which is not known beforehand,
which is why this flexibility is needed.


## Other

Currently does not work with zipped files, but it should be a simple fix. 
I am working on SQL support as well, but without robust security features.