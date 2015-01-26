## Exporting from a backend

1. Edit the defaults at the top of export.py: `DEFAULT_START` and `DEFAULT_END` 
indicate the date range to export sensor data. `DEFAULT_FILTERS` is a list of 
device filters to limit which devices should be exported from the backend.

2. Run the command:

    python export.py -n $BACKEND -k $KEY -s $SECRET

This creates two files: devices.json and datapoints.tsv.

## Importing to a backend

1. Make sure devices.json and datapoints.tsv exist in the same directory
as the import script.

2. Run the command:

    python import.py -n $BACKEND -k $KEY -s $SECRET

Note: datapoints can be split into multiple files, as long as they match
the regex `datapoints[0-9]*.tsv`