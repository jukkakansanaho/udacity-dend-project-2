# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cql_queries import *

def process_event_file(session, filepath):
    """Process input data (1 CSV file) and insert data into
        artists, songs, and users tables.

    Keyword arguments:
    * session --    reference to connected db.
    * filepath --   path to CSV file to be processed.

    Output:
    * songs and artists in a session data in song_in_session table.
    * artist, song, and user in a session in artist_in_session table.
    * user listening certain song in user_and_song table.
    """
    print('Starting to process input CSV data file...')
    # Set variables for counting processed rows for each table.
    num_lines_1 = 0
    num_lines_2 = 0
    num_lines_3 = 0
    # Process song_in_session table.
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(song_in_session_insert, \
                            (int(line[8]), \
                            int(line[3]), \
                            line[0], \
                            line[9], \
                            float(line[5])))
            num_lines_1 += 1
        print('Processed lines: ', num_lines_1)
        print('Data inserted successfully into song_in_session table.')

    # Process artist_in_session table
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(artist_in_session_insert, \
                            (int(line[10]), \
                            int(line[8]), \
                            line[0], \
                            line[9], \
                            int(line[3]), \
                            line[1], \
                            line[4]))
            num_lines_2 += 1
        print('Processed lines: ', num_lines_2)
        print('Data inserted successfully into artist_in_session table.')

    # Process user_and_song table
    with open(filepath, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(user_and_song_insert, \
                            (line[9], \
                            int(line[10]), \
                            line[1], \
                            line[4]))
            num_lines_3 += 1
        print('Processed lines: ', num_lines_3)
        print('Data inserted successfully into user_and_song table.')

def process_data(session, filepath, target_file, func):
    """Walk through the whole input data directory structure,
        combine input data into a new CSV file, call
        a separate function to handle the combined data.

    Keyword arguments:
    * session --        reference to connected db.
    * filepath --       path to CSV files to be processed
                        (./event_data).
    * target_file --    file into which the filtered data is written
    * func --           function to be called (process_event_data)

    Output:
    * Combined input data CSV file
    * console printouts of the data processing.
    """
    # Create a for loop to create a list of files and
    # collect each filepath
    file_path_list = []
    for root, dirs, files in os.walk(filepath):
        for f in files :
            file_path_list.append(os.path.abspath(f))
            print('Adding to filepath list: ' + f)
        # get total number of files found
        num_files = len(file_path_list)
        print('{} files found in {}\n'.format(num_files, filepath))

        file_path_list = glob.glob(os.path.join(root,'*'))
        #print(file_path_list)

    # initiating an empty list of rows that will be generated
    # from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

        # reading csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line)

    # uncomment the code below if you would like to get total number
    # of rows
    print('Total number of rows: ', len(full_data_rows_list))
    # uncomment the code below if you would like to check to see
    # what the list of event data rows will look like
    #print('Whole full_data_rows_list: ', full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_new
    # csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect(   'myDialect', \
                            quoting=csv.QUOTE_ALL, \
                            skipinitialspace=True)

    with open(target_file, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender',\
                    'itemInSession','lastName','length', \
                    'level','location','sessionId', 'song', \
                    'userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], \
                            row[6], row[7], row[8], row[12], row[13], \
                            row[16]))
        print('Input data files filtered successfully to: ' + target_file)

    # Insert pre-processed data from single CSV to Apache Cassandra DB.
    func(session, target_file)
    print('All input data processed and inserted to DB.')

def main():
    """Connect to DB and call process_data to walk through
        all the input data (./event_data/*.csv).

    Keyword arguments:
    * None

    Output:
    * All input data processed in DB tables.
    """
    # Get current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'
    target_file = 'event_datafile_new.csv'

    # Make a connection to Cassandra cluster
    from cassandra.cluster import Cluster
    try:
        # Connect to a local Cassandra cluster
        cluster = Cluster(['127.0.0.1'])
        # Set a session execute queries.
        session = cluster.connect()
    except Exception as e:
        print(e)

    try:
        # Set keyspace (sparkifydb) for Cassandra session.
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    process_data(   session, \
                    filepath, \
                    target_file, \
                    func=process_event_file)

    # Close the session and DB connection.
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    main()
