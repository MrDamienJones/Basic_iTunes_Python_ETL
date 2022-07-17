"""
Basic ETL For iTunes Export file.
"""
# Import modules
import boto3
import awswrangler as wr
import pandas as pd
from numpy import datetime64
import ETL_ITU_Play_Variables


# Create IAM Variables
aws_accesskey = ETL_ITU_Play_Variables.AWS_ACCESSKEY
aws_secret = ETL_ITU_Play_Variables.AWS_SECRET


# Create Boto3 session for AWS Wrangler
session = boto3.session.Session(aws_access_key_id = aws_accesskey,
                                aws_secret_access_key = aws_secret)

# Create S3 Variables
s3_bucket = ETL_ITU_Play_Variables.S3_BUCKET
s3_prefix = ETL_ITU_Play_Variables.S3_PREFIX


# Create S3 Path for AWS Wrangler
s3_path = f"s3://{s3_bucket}/{s3_prefix}"


# Function for rating conversion
def itunes_rating(r):
    """Converts ratings in export file to familiar format."""
    if r == 20:
        return 1
    elif r == 40:
        return 2
    elif r == 60:
        return 3
    elif r == 80:
        return 4
    elif r == 100:
        return 5
    else:
        return 0


# Creating DataFrame
print("Creating DataFrame.")

df = wr.s3.read_csv(path = s3_path,
                    path_suffix = ".csv",
                    boto3_session = session
                    )

print (f'DataFrame columns are {df.columns}')


# Deleting unnecessary DataFrame columns
print("Deleting unnecessary DataFrame columns.")

df = df.drop(columns =
    [
        'Time',
        'Bit Rate',
        'Skips',
        'Last Skipped',
        'Location'
    ]
)


# Renaming DataFrame columns
print("Renaming DataFrame columns.")

df = df.rename(columns =
    {
        'Name' : 'name',
        'Artist' : 'artist',
        'Album' : 'album',
        'Genre' : 'genre',
        'Track Number' : 'tracknumber',
        'Year' : 'year',
        'Date Modified' : 'datemodified',
        'Date Added' : 'dateadded',
        'Plays' : 'plays',
        'Last Played' : 'lastplayed',
        'My Rating' : 'myrating'
    }
)


# Reformatting DateTime DataFrame columns
print ('Reformatting DateTime DataFrame columns.')

df['datemodified'] = pd.to_datetime(df['datemodified'],yearfirst=False,dayfirst=True)
df['dateadded'] = pd.to_datetime(df['dateadded'],yearfirst=False,dayfirst=True)
df['lastplayed'] = pd.to_datetime(df['lastplayed'],yearfirst=False,dayfirst=True)


# Creating Date Columns From DateTime Columns
print('Creating Date Columns From DateTime Columns.')

df['datemodifieddate'] = df['datemodified'].dt.date
df['dateaddeddate'] = df['dateadded'].dt.date
df['lastplayeddate'] = df['lastplayed'].dt.date


# Creating MyRatingDigit Column using itunes_rating function
print('Creating MyRatingDigit Column.')

df['myratingdigit'] = df['myrating'].apply(itunes_rating)


# Replacing blank values to prevent IntCastingNaN errors
print('Replacing blank values to prevent IntCastingNaN errors.')

df['tracknumber'] = df['tracknumber'].fillna(0)
df['year'] = df['year'].fillna(0)
df['plays'] = df['plays'].fillna(0)
df['myrating'] = df['myrating'].fillna(0)


# Setting Data Types
print('Setting Data Types.')

df = df.astype(
    {
        'name' : str,
        'artist' : str,
        'album' : str,
        'genre' : str,
        'tracknumber' : int,
        'year' : int,
        'datemodified' : datetime64,
        'dateadded' : datetime64,
        'plays' : int,
        'lastplayed' : datetime64,
        'myrating' : int,
        'datemodifieddate' : datetime64,
        'dateaddeddate' : datetime64,
        'lastplayeddate' : datetime64,
        'myratingdigit' : int
    }
)

# Creating CSV of DataFrame
print(f'Creating CSV of DataFrame with columns {df.columns}')

df.to_csv('ETL_ITU_Play.csv')

print('Processes complete.')
