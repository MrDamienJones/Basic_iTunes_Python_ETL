# Basic_iTunes_Python_ETL

Basic_iTunes_Python_ETL is a Python ETL using AWS Data Wrangler that extracts data from an iTunes export file in Amazon S3, transforms the data in a DataFrame and loads to a CSV file.

The ETL performs the following actions:

- Creating DataFrame.
- Deleting unnecessary DataFrame columns.
- Renaming DataFrame columns.
- Reformatting DateTime DataFrame columns.
- Creating Date Columns From DateTime Columns.
- Creating MyRatingDigit Column.
- Replacing blank values to prevent IntCastingNaN errors.
- Setting Data Types.
- Creating CSV of DataFrame.

This document is supported by [Creating A Basic iTunes ETL With Python And AWS Data Wrangler](https://www.amazonwebshark.com/creating-a-basic-itunes-etl-with-python-and-aws-data-wrangler) on [amazonwebshark.com](https://www.amazonwebshark.com).

## Prerequisites 

This document assumes the following are already in place:

- Python version 3.7 or later (I used 3.9)
- A Python development environment with WSL (if using Windows).
- A virtual environment.
- An AWS IAM user with programmatic access.
- `requirements.txt` has been installed.

## Installation

1. Clone the repo.
1. Get the access keys for a user with programatic access to AWS.  The user must be able to read the s3 location containing the CSV file for the ETL.
1. Create a file named **ETL_ITU_Play_Variables.py** in the same folder as **ETL_ITU_Play.py**.
1. Create the following variables:

```
# Set variables : AWS Credentials
AWS_ACCESSKEY = 'MYACCESSKEY123456789'
AWS_SECRET = 'mysecretkey123456789'


# Set variables : S3 Paths
S3_BUCKET = 'my-s3-bucket-name-containing-the-csv'
S3_PREFIX = 'myfoldercontainingthecsv/mysubfoldercontainingthecsv/'
```

## Usage

When everything is in place, run the Python script.  Python will then move through the script, producing outputs as work is completed.  A typical example of a successful output is as follows:

```
Creating DataFrame.

DataFrame columns are Index(['Name', 'Artist', 'Album', 'Genre', 'Time', 'Track Number', 'Year',
       'Date Modified', 'Date Added', 'Bit Rate', 'Plays', 'Last Played',
       'Skips', 'Last Skipped', 'My Rating', 'Location'],
      dtype='object')

Deleting unnecessary DataFrame columns.

Renaming DataFrame columns.

Reformatting DateTime DataFrame columns.

Creating Date Columns From DateTime Columns.

Creating MyRatingDigit Column.

Replacing blank values to prevent IntCastingNaN errors.

Setting Data Types.

Creating CSV of DataFrame with columns Index(['name', 'artist', 'album', 'genre', 'tracknumber', 'year',
       'datemodified', 'dateadded', 'plays', 'lastplayed', 'myrating',
       'datemodifieddate', 'dateaddeddate', 'lastplayeddate', 'myratingdigit'],
      dtype='object')

Processes complete.
```


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss the changes.

Please bear in mind that this script is a work in progress.  I'm happy for this script (or any of the previous versions) to be used as a basis for similar scripts.


## License
[MIT](https://choosealicense.com/licenses/mit/)

## Credits

- README Template provided by [makeareadme.com](https://www.makeareadme.com/)