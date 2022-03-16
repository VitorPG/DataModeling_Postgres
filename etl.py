import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
   
    """
       This function is used to process the song files inside the given path and
       store the information into the song and artist tables.
       
       INPUTS:
       *cur = the cursor variable from the database
       * filepath = the location of the file that is being processed
       
   """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id","title","duration","year","artist_id"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_latitude','artist_longitude','artist_location']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
   
    """
       This function is used to process the log files in the given path and 
       store the information inside the time, users and songplay tables.
   
       INPUTS:
       *cur = the cursor variable from the database
       * filepath = the location of the file that is being processed
   
   """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    
    df.ts = pd.to_datetime(df['ts'],unit='ms')
    t = df['ts']
    
    # insert time data records
    
    time_data = [t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday]
    column_labels =  ('time_stamp','hour','day','week','month','year','day_of_week')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
   
    """
       This function creates a list of all the files inside a given path 
       and iterates over that list with a function given as argument. 
       
       INPUT: 
       *cur = the cursor variable from the database
       *conn = the connection variable to the database
       *filepath = the location of the file that is being processed
       *func = the function selected to process the files in filepath
       
   """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
         This procedure connects to the database, creates a cursor,
         defines the filepath and selects the functions that are going to be applied to each file
         of that path. 
         
         Once all the above steps are completed, the procedure closes the connection to the database.
         
         This function requires no INPUTS.
        
     """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
            print("Error: Could not connect to the DB")
            print(e) 
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the DB")
        print(e) 
        
    try: 
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    except psycopg2.Error as e:
        print("Error: Could not process song data")
        print(e)
        
    try:
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    except psycopg2.Error as e:
        print("Error: Could not process log data")
        print(e)

    conn.close()


if __name__ == "__main__":
    main()