# import
from googleapiclient.discovery import build
import psycopg2
import pandas as pd
import pymongo
import streamlit as st

#API Key
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

# Get data

client = pymongo.MongoClient("mongodb+srv://krithika:Guvi2024@cluster0.kmifik0.mongodb.net/")
db = client["Youtube_Data"]

def get_channel_details(channel_id):

        ch_details = get_channel(channel_id)
        pl_details = get_Playlist_details(channel_id)
        vid_ids = get_video_ids(channel_id)
        vi_details = get_video_details(vid_ids)
        comm_details = comment_details(vid_ids)
       
        col1 = db["Channel_Details"]
        col1.insert_one({
            "channel_Information": ch_details,
            "Playlist_Information": pl_details,
            "Video_Information": vi_details,
            "Comment_Information": comm_details
        })  

# Function to create channel table in PostgreSQL
def channel_table(selected_channel):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()

    try:
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
            mydb.commit()

    except:
            print("Channels table already created")

    single_channel_details= []
    col1 = db["Channel_Details"]
    for ch_data in col1.find({"channel_Information.channel_name": selected_channel}, {"_id": 0}):
        single_channel_details.append(ch_data["channel_Information"])
        
    df_single_channel = pd.DataFrame(single_channel_details)

    for index, row in df_single_channel.iterrows():
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
                        
        cursor.execute(insert_query, values)  
        mydb.commit()
    cursor.close()
    mydb.close()
    return 'Success'

# Function to create playlist table in PostgreSQL
def playlist_table(selected_channel):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()

    # Create the playlist_data table if it doesn't exist
    create_query = '''
        CREATE TABLE IF NOT EXISTS playlist_data (
            Playlist_ID VARCHAR(100) PRIMARY KEY,
            Title VARCHAR(80),
            Channel_ID VARCHAR(100),
            Channel_Name VARCHAR(100),
            Published_Date TIMESTAMP,
            Video_Count INT
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

    # Fetch data from MongoDB
    single_channel_details = []
    col1 = db["Channel_Details"]
    for ch_data in col1.find({"channel_Information.channel_name": selected_channel}, {"_id": 0}):
        single_channel_details.append(ch_data["Playlist_Information"])

    # Create DataFrame from MongoDB data
    df_single_channel = pd.DataFrame(single_channel_details[0])

    # Insert data into PostgreSQL table
    for index, row in df_single_channel.iterrows():
        insert_query = '''
            INSERT INTO playlist_data (Playlist_ID, Title, Channel_ID, Channel_Name, Published_Date, Video_Count)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        values = (
            row.get('Playlist_ID'),
            row.get('Title'),
            row.get('Channel_ID'),
            row.get('Channel_Name'),
            row.get('Published_Date'),
            row.get('Video_Count')
        )
        cursor.execute(insert_query, values)
        mydb.commit()

    cursor.close()
    mydb.close()

                       

    # Function to create video table in PostgreSQL
def video_table(selected_channel):
    mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor=mydb.cursor()

    create_query='''create table if not exists video_data(
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
    mydb.commit()

    single_channel_details= []
    col1 = db["Channel_Details"]
    for ch_data in col1.find({"channel_Information.channel_name": selected_channel}, {"_id": 0}):
        single_channel_details.append(ch_data["Video_Information"])
      
    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index, row in df_single_channel.iterrows():
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
        
        cursor.execute(insert_query, values)  
        mydb.commit()
    cursor.close()
    mydb.close()
    
        

# Function to create comment table in PostgreSQL
def comment_tables(selected_channel):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Guvi2024",
        database="Youtube_Data",
        port=5432
    )
    cursor = mydb.cursor()

    create_query = '''create table if not exists comment_data(Comment_ID varchar(100) primary key,
                    Video_ID varchar(80), 
                    Comments text, 
                    Comments_Author_ID varchar(100),
                    Comments_Published timestamp
                    )'''
    cursor.execute(create_query)
    mydb.commit()
   

    single_channel_details= []
    col1 = db["Channel_Details"]
    for ch_data in col1.find({"channel_Information.channel_name": selected_channel}, {"_id": 0}):
        single_channel_details.append(ch_data["Comment_Information"])
      
    df_single_channel= pd.DataFrame(single_channel_details[0])



    for index, row in df_single_channel.iterrows():

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
        cursor.execute(insert_query, values)  
        mydb.commit()
    cursor.close()
    mydb.close()

# =====================================================================

def migrate_to_sql(selected_channel):
 
    channel_table(selected_channel)
    playlist_table(selected_channel)
    video_table(selected_channel)
    comment_tables(selected_channel)
    
    return "Table created successfully"

# =========================================================================
# Function to create display channel table in PostgreSQL
def show_channel_table(client):
    ch_list = []
    db = client["Youtube_Data"]
    col1 = db["Channel_Details"]

    for ch_data in col1.find({}, {"_id": 0, "channel_Information": 1}):
        ch_list.append(ch_data["channel_Information"])

    df = st.dataframe(ch_list)
    return df

# Function to create  display playlist table in PostgreSQL
def show_playlist_table(client):
    pl_list = []
    db = client["Youtube_Data"]
    col1 = db["Channel_Details"]

    for pl_data in col1.find({}, {"_id": 0, "Playlist_Information": 1}):
        for i in range(len(pl_data["Playlist_Information"])):
            pl_list.append(pl_data["Playlist_Information"][i])
    df1 = st.dataframe(pl_list)
    return df1

# Function to create display video table in PostgreSQL
def show_video_table(client):
    vi_list=[]
    db=client["Youtube_Data"]
    col1=db["Channel_Details"]
    for vi_data in col1.find({},{"_id":0, "Video_Information":1}):
        for i in range(len(vi_data["Video_Information"])):
                vi_list.append(vi_data["Video_Information"][i])

    df2=st.dataframe(vi_list)
    return df2

# Function to create display comment table in PostgreSQL
def show_comment_table(client):
    com_list = []
    db=client["Youtube_Data"]
    col1 = db["Channel_Details"]
    for com_data in col1.find({}, {"_id": 0, "Comment_Information": 1}):
        for i in range(len(com_data["Comment_Information"])):
            com_list.append(com_data["Comment_Information"][i])

    df3 = st.dataframe(com_list)
    return df3
# =================================================================================================
# Streamli UI
   
st.markdown("<h1 style='text-align: center;'>YouTube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)

# Dropdown for actions
action = st.sidebar.selectbox("Select an action", ["Get Data", "Migrate Data", "Query Data"])

# Perform selected action
if action == "Get Data":
    # Get data button
    st.markdown("<h2>Get Data</h2>", unsafe_allow_html=True)
    channel_input = st.text_input('Enter the channel ID')
    if st.button("Get Data"):
        all_channels = []
        client = pymongo.MongoClient("mongodb+srv://krithika:Guvi2024@cluster0.kmifik0.mongodb.net/")
        db = client["Youtube_Data"]
        coll1 = db["Channel_Details"]
        for ch_data in coll1.find({}, {"_id": 0, "channel_Information": 1}):
            all_channels.append(ch_data["channel_Information"]["Channel_ID"])

        if channel_input in all_channels:
            st.warning('Channel details already exist, give another channel ID.')
        else:
            get_channel_details(channel_input)
            st.success('Channel details inserted successfully.')
    show_table = st.sidebar.selectbox("SELECT THE TABLE FOR VIEW", ["channels", "playlists", "videos", "comments"])

    if show_table == "channels":
        show_channel_table(client)
    elif show_table == "playlists":
        show_playlist_table(client)
    elif show_table == "videos":
        show_video_table(client)
    elif show_table == "comments":
        show_comment_table(client)        

elif action == "Migrate Data":
    # Migrate data button
    st.markdown("<h2>Migrate Data</h2>", unsafe_allow_html=True)
    all_channels = []
    client = pymongo.MongoClient("mongodb+srv://krithika:Guvi2024@cluster0.kmifik0.mongodb.net/")
    db = client["Youtube_Data"]
    coll1 = db["Channel_Details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_Information": 1}):
        all_channels.append(ch_data["channel_Information"]["channel_name"])

    selected_channel = st.selectbox("Select the channel to migrate", all_channels)

    if st.button("Migrate to SQL"):
        mydb = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="Guvi2024",
            database="Youtube_Data",
            port=5432
        )
        cursor = mydb.cursor()

        all_channels = []
        query = "SELECT channel_name FROM channel_data"
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            for i in result:
                all_channels.append(i[0])
        except Exception as e:
            st.error(f"An error occurred: {e}")

        if selected_channel in all_channels:
            st.error(f'{selected_channel} Already migrated to SQL.')
        else:
            table = migrate_to_sql(selected_channel)
            st.success(f'{selected_channel} - data migrated successfully to SQL.')

elif action == "Query Data":
  
    # SQL Query Output
    st.markdown("<h2>SQL Data</h2>", unsafe_allow_html=True)

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

    mydb=psycopg2.connect(host="localhost",
            user="postgres",
            password="Guvi2024",
            database="Youtube_Data",
            port="5432")
    cursor=mydb.cursor()

    if st.button("Run Query"):
        if selected_query == query_options[0]:
                query = '''SELECT Title AS Video_Title, Channel_Name FROM video_data'''
                cursor.execute(query)
                t = cursor.fetchall()
                df = pd.DataFrame(t, columns=["Video Title", "Channel Name"])
                st.write(df)

        elif selected_query == query_options[1]:
            query = '''SELECT Channel_Name, COUNT(*) AS Video_Count
                    FROM video_data
                    GROUP BY Channel_Name
                    ORDER BY Video_Count DESC
                    LIMIT 1'''
            cursor.execute(query)
            t = cursor.fetchall()
            df = pd.DataFrame(t, columns=["Channel Name", "Video Count"])
            st.write(df)

        elif selected_query == query_options[2]:
            query = '''SELECT Title AS Video_Title, Channel_Name, Views
                    FROM video_data
                    ORDER BY Views DESC
                    LIMIT 10'''
            cursor.execute(query)
            t = cursor.fetchall()
            df = pd.DataFrame(t, columns=["Video Title", "Channel Name", "Views"])
            st.write(df)

        elif selected_query == query_options[3]:
            query = '''SELECT v.Video_ID,v.Title AS Video_Name, COUNT(*) AS Comment_Count
                FROM video_data v
                JOIN comment_data c ON v.Video_ID = c.Video_ID
                GROUP BY v.Video_ID, v.Title'''
            cursor.execute(query)
            t = cursor.fetchall()
            df = pd.DataFrame(t, columns=["Video ID", "Video Name", "Comment Count"])
            st.write(df)

        elif selected_query == query_options[4]:
            query4 = '''SELECT Title AS Video_Title, Channel_Name, Likes
                        FROM video_data
                        ORDER BY Likes DESC
                        '''
            cursor.execute(query4)
            mydb.commit()
            t4 = cursor.fetchall()
            df4 = pd.DataFrame(t4, columns=["Video Title", "Channel Name", "Likes"])
            df4['Likes']=df4['Likes'].fillna(0)
            df4=df4.sort_values('Likes',ascending=False)
            df4 = df4.reset_index(drop=True)
            st.write(df4)

        elif selected_query == query_options[5]:
            query5 = '''SELECT Title AS Video_Title, SUM(Likes) AS Total_Likes
                        FROM video_data
                        GROUP BY Title'''
            cursor.execute(query5)
            mydb.commit()
            t5 = cursor.fetchall()
            df5 = pd.DataFrame(t5, columns=["Video Title", "Total Likes"])
            st.write(df5)

        elif selected_query == query_options[6]:
            query6 = '''SELECT Channel_Name, SUM(Views) AS Total_Views
                        FROM video_data
                        GROUP BY Channel_Name'''
            cursor.execute(query6)
            mydb.commit()
            t6 = cursor.fetchall()
            df6 = pd.DataFrame(t6, columns=["Channel Name", "Total Views"])
            st.write(df6)

        elif selected_query == query_options[7]:
            query7 = '''SELECT Title AS Video_Title, Channel_Name, Published_Date
                        FROM video_data
                        WHERE EXTRACT(YEAR FROM Published_Date) = 2022'''
            cursor.execute(query7)
            mydb.commit()
            t7 = cursor.fetchall()
            df7 = pd.DataFrame(t7, columns=["Video Title", "Channel Name", "Published Date"])
            st.write(df7)

        elif selected_query == query_options[8]:
            query9 = '''SELECT Channel_Name, AVG(Video_Duration) AS Avg_Duration
                        FROM video_data
                        GROUP BY Channel_Name'''
            cursor.execute(query9)
            mydb.commit()
            t9 = cursor.fetchall()
            df9 = pd.DataFrame(t9, columns=["Channel Name", "Avg Duration"])
            st.write(df9)

        elif selected_query == query_options[9]:
            query10 = '''SELECT v.Title AS Video_Name, v.Channel_Name, COUNT(*) AS Comment_Count
                        FROM video_data v
                        JOIN comment_data c ON v.Video_ID = c.Video_ID
                        GROUP BY v.Title, v.Channel_Name
                        ORDER BY Comment_Count DESC'''
            cursor.execute(query10)
            mydb.commit()
            t10 = cursor.fetchall()
            df10 = pd.DataFrame(t10, columns=["Video Title", "Channel Name", "Comment Count"])
            st.write(df10)
    pass
