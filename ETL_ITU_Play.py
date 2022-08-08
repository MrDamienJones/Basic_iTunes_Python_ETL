"""
Basic ETL For iTunes Export file.
"""
# Import modules
import boto3
import awswrangler as wr
import pandas as pd
from numpy import datetime64
import ETL_ITU_Play_Variables as ETLV


# Create Boto3 session for AWS Wrangler
session = boto3.session.Session(aws_access_key_id = ETLV.AWS_ACCESSKEY,
                                aws_secret_access_key = ETLV.AWS_SECRET,
                                region_name = 'eu-west-1')


# Create S3 RAW path for AWS Wrangler
s3_path_raw = f"s3://{ETLV.S3_BUCKET_RAW}/{ETLV.S3_PREFIX_RAW}"


# Create S3 Parquet path for AWS Wrangler
s3_path_ingested = f"s3://{ETLV.S3_BUCKET_INGESTED}/{ETLV.S3_PREFIX_INGESTED}/itunesdata.parquet"


# Function for rating conversion
def itunes_rating(rating):
    """Converts ratings in export file to familiar format."""
    if rating == 20:
        return 1
    elif rating == 40:
        return 2
    elif rating == 60:
        return 3
    elif rating == 80:
        return 4
    elif rating == 100:
        return 5
    else:
        return 0


class BasiciTunesETL():
    """
    Class for extracting iTunes CSV data and exporting to Parquet.
    """

    # Creating DataFrame
    print("Creating DataFrame.")

    df = wr.s3.read_csv(path = s3_path_raw,
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


    # Creating Parquet file from DataFrame
    print('Creating Parquet file from DataFrame.')

    wr.s3.to_parquet(
        df = df,
        boto3_session = session,
        path = s3_path_ingested
    )

    print('Processes complete.')
