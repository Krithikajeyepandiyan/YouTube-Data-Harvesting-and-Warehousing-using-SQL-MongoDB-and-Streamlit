# import
from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import pymongo
import streamlit as st

#
def api_connect():
    api_key = 'AIzaSyCw6kmsoiD2Iy6wOP9EOkXtLoDVL2ys2oM'
    youtube = build("youtube", "v3", developerKey=api_key)
   
    return youtube  

youtube=api_connect()

# channel Info
def get_channel(channel_id):
    req = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    ) 
    response = req.execute()

    for i in response['items']:
        data = {
            'channel_name': i['snippet']['title'],
            'Channel_ID':i['id'],
            'Channel_Description': i['snippet']['description'],
            'Views': i['statistics']['viewCount'],
            'Subscribers': i['statistics']['subscriberCount'],
            'Total_videos': i['statistics']['videoCount'],
            'Playlist_id': i['contentDetails']['relatedPlaylists']['uploads'],
            'Likes': i['contentDetails']['relatedPlaylists']['likes']
        }
       
   
    return data
   
# get video ids
def get_video_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id=channel_id,
            part="contentDetails").execute()
           
    Playlist_id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']


    nextpagetoken=None


    while True:
        req1=youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=Playlist_id,
            maxResults=50,
            pageToken=nextpagetoken
        ).execute()


        for i in range(len(req1['items'])):
            video_ids.append(req1['items'][i]['snippet']['resourceId']['videoId'])
        nextpagetoken=req1.get('nextPageToken')


        if nextpagetoken is None:
            break
    return video_ids


# get video_details
def get_video_details(video_ids):
    video_data=[]
    try:
        for video_id in video_ids:
            req=youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            ).execute()


            for i in  req['items']:
                data={
                    'Channel_Name': i ['snippet']['channelTitle'],
                    'Channel_ID': i ['snippet']['channelId'],
                    'Video_ID': i['id'],
                    'Title':i['snippet']['title'],
                    'Tags':i ['snippet'].get('tags'),
                    'Thumbnail':i['snippet']['thumbnails']['default']['url'],
                    'Description':i['snippet'].get('description'),
                    'Published_Date':i['snippet']['publishedAt'],
                    'Video_Duration':i['contentDetails']['duration'],
                    'Views':i['statistics'].get('viewCount'),
                    'Likes':i['statistics'].get('likeCount'),
                    'Favorite_Count': i['statistics']['favoriteCount'],
                    'Comments_Count':i ['statistics'].get('commentCount'),
                    'Definition':i['contentDetails']['definition'],
                    'Caption_Status':i['contentDetails']['caption']
                    }


                video_data.append(data)
    except:
        pass
   
    return video_data


# comment Info
def comment_details(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            req=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            ).execute()


            for i in req['items']:
                comment = i['snippet']['topLevelComment']
                data = {
                    'Comment_ID': comment['id'],
                    'Video_ID': comment['snippet']['videoId'],
                    'Comments': comment['snippet']['textDisplay'],
                    'Comments_Author_ID': comment['snippet']['authorChannelId']['value'],
                    'Comments_Published': comment['snippet']['publishedAt']
                }
                comment_data.append(data)
    except:
        pass
    return comment_data


# Playlist_details
def get_Playlist_details(channel_id):
    Playlist_data=[]
    nextpagetoken=None

    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=nextpagetoken
        ).execute()


        for i in request['items']:
            data={
                'Playlist_ID':i['id'],
                'Title':i['snippet']['title'],
                'Channel_ID':i['snippet']['channelId'],
                'Channel_Name':i['snippet']['channelTitle'],
                'Published_Date':i['snippet']['publishedAt'],
                'Video_Count':i['contentDetails']['itemCount']
            }
            Playlist_data.append(data)
        nextpagetoken=request.get('nextPageToken')


        if nextpagetoken is None:
                break
    return Playlist_data

# ==============================================================================================================================================
# Get data

client = pymongo.MongoClient("mongodb+srv://krithika:Guvi2024@cluster0.kmifik0.mongodb.net/")
db = client["Youtube_Data"]

# Initialize session state
if "channels_for_migration" not in st.session_state:
    st.session_state["channels_for_migration"] = []

def channel_exists(channel_id):
    col = db["Channel_Details"]
    return col.find_one({"Channel_ID": channel_id}) is not None

def get_channel_details(channel_id):
    if channel_exists(channel_id):
        return "Channel already exists in the database"
    else:
        ch_details = get_channel(channel_id)
        pl_details = get_Playlist_details(channel_id)
        vid_ids = get_video_ids(channel_id)
        vi_details = get_video_details(vid_ids)
        comm_details = comment_details(vid_ids)

        # Print channel details before insertion
        print("Channel Details:", ch_details)
        print("Playlist Details:", pl_details)
        print("Video Details:", vi_details)
        print("Comment Details:", comm_details)
       
        col1 = db["Channel_Details"]
        col1.insert_one({
            "channel_Information": ch_details,
            "Playlist_Information": pl_details,
            "Video_Information": vi_details,
            "Comment_Information": comm_details
        })  

        # Add the channel ID to the session state list
        st.session_state.channels_for_migration.append(channel_id)
       
        return "Channel details fetched and inserted successfully"



# ===========================================================================================================================================

# Initialize session state
# if "channels_for_migration" not in st.session_state:
#     st.session_state["channels_for_migration"] = []

# # MongoDB client initialization
# client = pymongo.MongoClient("mongodb+srv://krithika:Guvi2024@cluster0.kmifik0.mongodb.net/")
# db = client["Youtube_Data"]

# # Function to check if a channel already exists
# # def channel_exists(channel_id):
# #     col = db["Channel_Details"]
# #     return col.find_one({"Channel_ID": channel_id}) is not None

# def channel_exists_in_postgres(channel_id):
#     mydb, cursor = connect_to_postgres()
#     cursor.execute("SELECT COUNT(*) FROM channel_data WHERE channel_ID = %s", (channel_id,))
#     count = cursor.fetchone()[0]
#     cursor.close()
#     mydb.close()
#     return count > 0

# ===========================================================================================================================================
# Function to insert channel data into PostgreSQL

def insert_channel_data():
   
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()  

    drop_query = '''drop table if exists channel_data'''
    cursor.execute(drop_query)

    create_query = """create table if not exists channel_data (
        channel_name varchar(200),
        channel_ID varchar(100) primary key,
        channel_description text,
        views bigint,
        subscribers bigint,
        total_videos bigint,
        playlist_id varchar(100)
    )"""
    cursor.execute(create_query)


    ch_list = []
    col1 = db["Channel_Details"]
    for ch_data in col1.find({}, {"_id": 0, "channel_Information": 1}):
        ch_list.append(ch_data["channel_Information"])
       
    df = pd.DataFrame(ch_list)


    for index, row in df.iterrows():
        insert_query = '''insert into channel_data (channel_name, channel_ID, channel_description, views, subscribers, total_videos, playlist_id)
                        values (%s, %s, %s, %s, %s, %s, %s)'''
       
        values = (
            row['channel_name'],
            row['Channel_ID'],
            row['Channel_Description'],
            row['Views'],
            row['Subscribers'],
            row['Total_videos'],
            row['Playlist_id']
        )
        try:                    
            cursor.execute(insert_query, values)  
        except:
            print("Channel values are already inserted")
    cursor.close()
    mydb.close()


# Function to create playlist table in PostgreSQL
def playlist_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()  

    drop_query = "drop table if exists playlist_data"
    cursor.execute(drop_query)

    create_query = '''create table if not exists playlist_data (
        Playlist_ID varchar(100) primary key,
        Title varchar(80),
        Channel_ID varchar(100),
        Channel_Name varchar(100),
        Published_Date timestamp,
        Video_Count int
    )'''
    cursor.execute(create_query)


    pl_list = []
    col1 = db["Channel_Details"]
    for pl_data in col1.find({}, {"_id": 0, "Playlist_Information": 1}):
        for i in range(len(pl_data["Playlist_Information"])):
            pl_list.append(pl_data["Playlist_Information"][i])


    df1 = pd.DataFrame(pl_list)


    for index, row in df1.iterrows():
        insert_query = '''insert into playlist_data (Playlist_ID, Title, Channel_ID, Channel_Name, Published_Date, Video_Count)
                            values (%s, %s, %s, %s, %s, %s)'''
           
        values = (
            row['Playlist_ID'],
            row['Title'],
            row['Channel_ID'],
            row['Channel_Name'],
            row['Published_Date'],
            row['Video_Count']
        )
           
        try:
            cursor.execute(insert_query, values)
        except:
            print("Playlist values are already inserted")                
    cursor.close()
    mydb.close()


# Function to create video table in PostgreSQL
def video_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()

    drop_query = "drop table if exists video_data"
    cursor.execute(drop_query)


    create_query = '''create table if not exists video_data(
        Channel_Name varchar(200),
        Channel_ID varchar(100),
        Video_ID varchar(100) primary key,
        Title varchar(150),
        Tags text,
        Thumbnail varchar(300),
        Description text,
        Published_Date timestamp,
        Video_Duration interval,
        Views bigint,
        Likes bigint,
        Favorite_Count int,
        Comments_Count int,
        Definition varchar(20),
        Caption_Status varchar(50)
    ) '''
    cursor.execute(create_query)


    vi_list = []
    col1 = db["Channel_Details"]
    for vi_data in col1.find({}, {"_id": 0, "Video_Information": 1}):
        for i in range(len(vi_data["Video_Information"])):
            vi_list.append(vi_data["Video_Information"][i])


    df2 = pd.DataFrame(vi_list)


    for index, row in df2.iterrows():
        insert_query = '''insert into video_data (
            Channel_Name,
            Channel_ID,
            Video_ID,
            Title,
            Tags,
            Thumbnail,
            Description,
            Published_Date,
            Video_Duration,
            Views,
            Likes,
            Favorite_Count,
            Comments_Count,
            Definition,
            Caption_Status
        )
        Values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (
            row['Channel_Name'],
            row['Channel_ID'],
            row['Video_ID'],
            row['Title'],
            row['Tags'],
            row['Thumbnail'],
            row['Description'],
            row['Published_Date'],
            row['Video_Duration'],
            row['Views'],
            row['Likes'],
            row['Favorite_Count'],
            row['Comments_Count'],
            row['Definition'],
            row['Caption_Status']
        )
       
        try:
            cursor.execute(insert_query, values)
        except:
             print("Video values already inserted in the table")
    cursor.close()
    mydb.close()


# Function to create comment table in PostgreSQL
def comment_table():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()


    drop_query = "drop table if exists comment_data"
    cursor.execute(drop_query)


    create_query = '''create table if not exists comment_data(
        Comment_ID varchar(100) primary key,
        Video_ID varchar(80),
        Comments text,
        Comments_Author varchar(150),
        Comments_Author_ID varchar(100),
        Comments_Published timestamp
    )'''
    cursor.execute(create_query)


    com_list = []
    col1 = db["Channel_Details"]
    for com_data in col1.find({}, {"_id": 0, "Comment_Information": 1}):
        for i in range(len(com_data["Comment_Information"])):
            com_list.append(com_data["Comment_Information"][i])


    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
        insert_query= '''insert into comment_data(Comment_ID ,
                        Video_ID ,
                        Comments ,
                        Comments_Author_ID ,
                        Comments_Published
                        )
                        values(%s, %s, %s, %s, %s)'''
        values=(
            row['Comment_ID'],
            row['Video_ID'],
            row['Comments'],
            row['Comments_Author_ID'],
            row['Comments_Published']
        )
        try:
            cursor.execute(insert_query,values)
        except:
            print("Comment values already inserted in the table")

    cursor.close()
    mydb.close()

# =========================================================================================================

def show_channel_table(client):
    ch_list = []
    db = client["Youtube_Data"]
    col1 = db["Channel_Details"]

    for ch_data in col1.find({}, {"_id": 0, "channel_Information": 1}):
        ch_list.append(ch_data["channel_Information"])

    df = pd.DataFrame(ch_list)
    st.subheader("Channel Table")
    st.write(df)


def show_playlist_table(client):
    pl_list = []
    db = client["Youtube_Data"]
    col1 = db["Channel_Details"]

    for pl_data in col1.find({}, {"_id": 0, "Playlist_Information": 1}):
        for playlist in pl_data["Playlist_Information"]:
            pl_list.append(playlist)


    df = pd.DataFrame(pl_list)
    st.subheader("Playlist Table")
    st.write(df)


def show_video_table(client):
    vi_list = []
    db = client["Youtube_Data"]
    col1 = db["Channel_Details"]

    for vi_data in col1.find({}, {"_id": 0, "Video_Information": 1}):
        for video in vi_data["Video_Information"]:
            vi_list.append(video)

    df = pd.DataFrame(vi_list)
    st.subheader("Video Table")
    st.write(df)


def show_comment_table(client):
    com_list = []
    db = client["Youtube_Data"]
    col1 = db["Channel_Details"]

    for com_data in col1.find({}, {"_id": 0, "Comment_Information": 1}):
        for comment in com_data["Comment_Information"]:
            com_list.append(comment)


    df = pd.DataFrame(com_list)
    st.subheader("Comment Table")
    st.write(df)

# def fetch_existing_channels():
#     mydb, cursor = connect_to_postgres()
#     cursor.execute("SELECT channel_name FROM channel_data")
#     channels = [row[0] for row in cursor.fetchall()]
#     cursor.close()
#     mydb.close()
#     return channels

# ===============================================================================================
    
# Function to migrate data from MongoDB to PostgreSQL
def migrate_data_to_postgres():
    mydb, cursor = connect_to_postgres()
    insert_channel_data()
    playlist_table()
    video_table()
    comment_table()
    cursor.close()
    mydb.close()

# =======================================================================================================
# Function to establish a connection and create a cursor
def connect_to_postgres():
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()
    return mydb, cursor

# def connect_to_mongodb():
#     client = pymongo.MongoClient("mongodb+srv://krithika:Guvi2024@cluster0.kmifik0.mongodb.net/")
#     db = client["Youtube_Data"]
#     col1=db["Channel_Details"]

# def fetch_mongodb_data(collection_name):
#     db = connect_to_mongodb()
#     col1 = db[collection_name]
#     data = list(col1.find())
#     return data

# Function to fetch data from PostgreSQL for channel data
def fetch_channel_data():
    mydb, cursor = connect_to_postgres()
    cursor.execute("SELECT * FROM channel_data")
    data = cursor.fetchall()
    cursor.close()
    mydb.close()
    return data

# Function to fetch data from PostgreSQL for playlist data
def fetch_playlist_data():
    mydb, cursor = connect_to_postgres()
    cursor.execute("SELECT * FROM playlist_data")
    data = cursor.fetchall()
    cursor.close()
    mydb.close()
    return data


# Function to fetch data from PostgreSQL for video data
def fetch_video_data():
    mydb, cursor = connect_to_postgres()
    cursor.execute("SELECT * FROM video_data")
    data = cursor.fetchall()
    cursor.close()
    mydb.close()
    return data


# Function to fetch data from PostgreSQL for comment data
def fetch_comment_data():
    mydb, cursor = connect_to_postgres()
    cursor.execute("SELECT * FROM comment_data")
    data = cursor.fetchall()
    cursor.close()
    mydb.close()
    return data


# Function to display data in Streamlit
def show_data(data, title, column_names):
    st.header(title)
    if data:
        df = pd.DataFrame(data, columns=column_names)
        st.dataframe(df)
    else:
        st.write(f"No {title.lower()} data found.")

# ========================================================================


def connect_to_database():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )


# Function to execute SQL queries
def execute_query(query):
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return pd.DataFrame(rows)


# Function to display videos and their corresponding channels
def show_videos_and_channels():
    query = "SELECT Title AS Video_Title, Channel_Name FROM video_data"
    df = execute_query(query)
    df.columns = ["Video Title", "Channel Name"]
    return df


# Function to display channels with the most number of videos
def show_channels_with_most_videos():
    query = """
        SELECT Channel_Name, COUNT(*) AS Video_Count
        FROM video_data
        GROUP BY Channel_Name
        ORDER BY Video_Count DESC
        LIMIT 1
        """
    df = execute_query(query)
    df.columns = ["Channel Name", "Video Count"]
    return df


    # Function to display top 10 most viewed videos and their respective channels
def show_top_10_most_viewed_videos():
    query = """
        SELECT Title AS Video_Title, Channel_Name, Views
        FROM video_data
        ORDER BY Views DESC
        LIMIT 10
    """
    df = execute_query(query)
    df.columns = ["Video Title", "Channel Name", "Views"]
    return df


    # Function to display number of comments on each video and their corresponding video names
def show_comments_per_video():
    query = """
        SELECT v.Video_ID,v.Title AS Video_Name, COUNT(*) AS Comment_Count
        FROM video_data v
        JOIN comment_data c ON v.Video_ID = c.Video_ID
        GROUP BY v.Video_ID, v.Title

    """
    df = execute_query(query)
    df.columns = ["Video ID", "Video Name", "Comment Count"]
    return df
# Function to display videos with the highest number of likes and their corresponding channels
def show_videos_with_highest_likes():
        query = """
            SELECT Title AS Video_Title, Channel_Name, Likes
            FROM video_data
            ORDER BY Likes DESC
            LIMIT 1
        """
        df = execute_query(query)
        df.columns = ["Video Title", "Channel Name", "Likes"]
        return df


    # Function to display total number of likes and dislikes for each video and their corresponding names
def show_likes_and_dislikes_per_video():
        query = """
            SELECT Title AS Video_Title, SUM(Likes) AS Total_Likes
            FROM video_data
            GROUP BY Title
        """
        df = execute_query(query)
        df.columns = ["Video Title", "Total Likes"]
        return df


    # Function to display total number of views for each channel and their corresponding names
def show_total_views_per_channel():
        query = """
            SELECT Channel_Name, SUM(Views) AS Total_Views
            FROM video_data
            GROUP BY Channel_Name
        """
        df = execute_query(query)
        df.columns = ["Channel Name", "Total Views"]
        return df
# Function to display names of channels that published videos in 2022
def show_channels_with_videos_in_2022():
        query = """
            SELECT DISTINCT Channel_Name
            FROM video_data
            WHERE EXTRACT(YEAR FROM Published_Date) = 2022
        """
        df = execute_query(query)
        df.columns = ["Channel Name"]
        return df


    # Function to display average duration of all videos in each channel and their corresponding names
def show_average_duration_per_channel():
        query = """
            SELECT Channel_Name, AVG(Video_Duration) AS Avg_Duration
            FROM video_data
            GROUP BY Channel_Name
        """
        df = execute_query(query)
        df.columns = ["Channel Name", "Avg Duration"]
        return df

    # Function to display videos with the highest number of comments and their corresponding channels
def show_videos_with_highest_comments():
        query = """
            SELECT v.Title AS Video_Name, v.Channel_Name, COUNT(*) AS Comment_Count
            FROM video_data v
            JOIN comment_data c ON v.Video_ID = c.Video_ID
            GROUP BY v.Title, v.Channel_Name
            ORDER BY Comment_Count DESC
            LIMIT 1

        """
        df = execute_query(query)
        df.columns = ["Video Title", "Channel Name", "Comment Count"]
        return df

# =========================================================

def main():
    # Initialize session state for storing channel list
    if "channels_for_migration" not in st.session_state:
        st.session_state.channels_for_migration = []


    st.title("YouTube Data Analysis")


    # Sidebar
    action = st.sidebar.selectbox("Choose action", ["Get Data", "Migrate Data", "Query Data"])


    if action == "Get Data":
         st.sidebar.subheader("Enter Channel ID or Link")
         channel_input = st.sidebar.text_input("Enter Channel ID or Link")
         existing_channel_names = [channel[0] for channel in fetch_channel_data()]

            # Check if the entered channel name already exists
         if channel_input in existing_channel_names:
                st.sidebar.warning("Channel already exists in the database.")
         else:
                
                message = get_channel_details(channel_input)
                st.sidebar.text(message)

                # Display table selection dropdown
                show_table = st.sidebar.selectbox("SELECT THE TABLE FOR VIEW", ["channels", "playlists", "videos", "comments"])
                if show_table == "channels":
                        show_channel_table(client)
                elif show_table == "playlists":
                    show_playlist_table(client)
                elif show_table == "videos":
                    show_video_table(client)
                elif show_table == "comments":
                    show_comment_table(client)
            #     st.sidebar.subheader("Enter Channel ID or Link")
            #     channel_input = st.sidebar.text_input("Enter Channel ID or Link")


    #     if channel_input:
    #         message = get_channel_details(channel_input)
    #         st.sidebar.text(message)
       
    #     show_table = st.sidebar.selectbox("SELECT THE TABLE FOR VIEW", ["channels", "playlists", "videos", "comments"])


    #     if show_table == "channels":
    #         show_channel_table(client)
    #     elif show_table == "playlists":
    #         show_playlist_table(client)
    #     elif show_table == "videos":
    #         show_video_table(client)
    #     elif show_table == "comments":
    #         show_comment_table(client)


    

    elif action == "Migrate Data":
        # st.sidebar.subheader("Select Channel to Migrate")
        channels_to_migrate = fetch_channel_data()
        st.title("Data Migration")

        if channels_to_migrate:
            channel_names = [channel[0] for channel in channels_to_migrate]
            selected_channel = st.selectbox("Choose Channel", channel_names)

            # Check if the selected channel already exists in the database
            if selected_channel in st.session_state.channels_for_migration:
                st.sidebar.warning("Selected channel already exists in the database.")
            else:
                if st.sidebar.button("Migrate"):
                    migrate_data_to_postgres()
                    # Append the selected channel to the list
                    st.session_state.channels_for_migration.append(selected_channel)
                    st.write(f"{selected_channel} - Data Successfully Migrated")

        else:
            st.warning("No channels available for migration.")

    elif action == "Query Data":
        st.sidebar.subheader("Select Channel to Query")
        st.title("SQL Query Output")

    # Check if there are channels available for querying
        if st.session_state.channels_for_migration:
            selected_channel = st.sidebar.selectbox("Choose Channel", st.session_state.channels_for_migration)
            query_options = [
                "1. Names of all videos and their corresponding channels",
                "2. Channels with the most number of videos",
                "3. Top 10 most viewed videos and their respective channels",
                "4. Number of comments on each video and their corresponding video names",
                "5. Videos with the highest number of likes and their corresponding channel names",
                "6. Total number of likes for each video and their corresponding video names",
                "7. Total number of views for each channel and their corresponding channel names",
                "8. Names of all channels that have published videos in the year 2022",
                "9. Average duration of all videos in each channel and their corresponding channel names",
                "10. Videos with the highest number of comments and their corresponding channel names"
            ]

            selected_query = st.selectbox("Please Select a Query", query_options)

            if st.button("Run Query"):

                if selected_query == query_options[0]:
                            st.subheader("Names of all videos and their corresponding channels")
                            st.write(show_videos_and_channels())

                elif selected_query == query_options[1]:
                            st.subheader("Channels with the most number of videos")
                            st.write(show_channels_with_most_videos())

                elif selected_query == query_options[2]:
                            st.subheader("Top 10 most viewed videos and their respective channels")
                            st.write(show_top_10_most_viewed_videos())

                elif selected_query == query_options[3]:
                    st.subheader("Number of comments on each video and their corresponding video names")
                    st.write(show_comments_per_video())

                elif selected_query == query_options[4]:
                    st.subheader("Videos with the highest number of likes and their corresponding channel names")
                    st.write(show_videos_with_highest_likes())

                elif selected_query == query_options[5]:
                    st.subheader("Total number of likes and dislikes for each video and their corresponding video names")
                    st.write(show_likes_and_dislikes_per_video())

                elif selected_query == query_options[6]:
                    st.subheader("Total number of views for each channel and their corresponding channel names")
                    st.write(show_total_views_per_channel())

                elif selected_query == query_options[7]:
                    st.subheader("Names of all channels that have published videos in the year 2022")
                    st.write(show_channels_with_videos_in_2022())

                elif selected_query == query_options[8]:
                    st.subheader("Average duration of all videos in each channel and their corresponding channel names")
                    st.write(show_average_duration_per_channel())

                elif selected_query == query_options[9]:
                    st.subheader("Videos with the highest number of comments and their corresponding channel names")
                    st.write(show_videos_with_highest_comments())
                pass
                
            
          
                # Run selected query for the selected channel
            # st.sidebar.text(f"Running {selected_query} for {selected_channel}")
           


if __name__ == "__main__":
    main()
