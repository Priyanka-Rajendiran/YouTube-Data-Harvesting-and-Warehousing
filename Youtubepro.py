from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st

def convert_dur(s):
  l = []
  f = ''
  for i in s:
    if i.isnumeric():
      f = f+i
    else:
      if f:
        l.append(f)
        f = ''
  if 'H' not in s:
    l.insert(0,'00')
  if 'M' not in s:
    l.insert(1,'00')
  if 'S' not in s:
    l.insert(-1,'00')
  return ':'.join(l)

api_key="AIzaSyCCX_cPI_mlpMcSiEb2-L3wo75i5TDzo9Q"
channel_id = "UCnz-ZXXER4jOvuED5trXfEA"  #techTFQ

youtube = build("youtube","v3",developerKey=api_key)


#Fuction to Get Channel Detail
def get_channel_det(youtube,channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    
    data = {
        'Channel_Name' : response ['items'][0]['snippet']['title'],
        'Channel_Id': response ['items'][0]['id'],
        'Subscription_Count' : response ['items'][0]['statistics']['subscriberCount'],
        'Channel_Views' : response ['items'][0]['statistics']['viewCount'],
        'Channel_Description': response ['items'][0]['snippet']['description'],
        'Total_Videos' : response ['items'][0]['statistics']['videoCount'],
        'Playlist_id' : response ['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    
    return data

#Function To Get Video IDs
def get_all_video_ids(youtube, channel_id):

    channel_request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    channel_response = channel_request.execute()

    uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    
    video_ids = []
    next_page_token = None

    while True:
        playlist_request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        playlist_response = playlist_request.execute()

        for item in playlist_response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = playlist_response.get('nextPageToken')

        if not next_page_token:
            break  # Break the loop if there are no more pages

    return video_ids



#Function to Get Video Details
def get_video_details(youtube, Video_ids):
    all_video_stats = []
    for i in range(0, len(Video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(Video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            video_det = dict(
                Channel_Name=video['snippet']['channelTitle'],
                Channel_Id=video['snippet']['channelId'],
                Video_id=video['id'],
                Video_Name=video['snippet']['title'],
                video_des=video['snippet'].get('description'),
                Tags=",".join(video['snippet'].get('tags',[])),  
                Published_at=(video['snippet']['publishedAt'])[:-1],
                View_count=video['statistics'].get('viewCount'),
                Like_count=video['statistics']['likeCount'],
                Fav_count=video['statistics']['favoriteCount'],
                Comment_count=video['statistics'].get('commentCount'),
                Duration=convert_dur(video['contentDetails']['duration']),
                Thum_nail=video['snippet']['thumbnails']['default']['url'],
                Caption_status=video['contentDetails']['caption']
            )
            all_video_stats.append(video_det)

    return all_video_stats  



#Function to Get Comment Details
def get_commend_details(youtube, Video_ids):
    comment_data=[]
    try:
        for video_id in Video_ids:
            request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50
            )
            response = request.execute()

            for comment in response['items']:
                comment_det =dict(Comment_Id= comment ['id'],
                                video_id=comment['snippet']['topLevelComment']['snippet']['videoId'],
                                Comment_Text= comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                Comment_Author= comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                Com_publishedDate =(comment['snippet']['topLevelComment']['snippet']['publishedAt'])[:-1])

                comment_data.append(comment_det)
    except:
        pass
    return comment_data

#Function to Get Playlits Details
def get_playlist_det(channel_id):
    next_page_token=None
    Playlist_det=[]
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']:
            playlist= dict(Playlist_id=item['id'],
                          Playlist_Name=item['snippet']['title'],
                          Channel_id= item['snippet']['channelId'],
                          Channel_Name=item['snippet']['channelTitle'],
                          PublishedAt= (item['snippet']['publishedAt'][:-1]),
                          Video_count=item['contentDetails']['itemCount'])
            Playlist_det.append(playlist)
            
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return Playlist_det

#Connectiono To MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017')
mydb = client["Youtube_Data"]  #Creating Data Base

from pymongo.errors import DuplicateKeyError

def channel_details(channel_id):
    existing_document = mydb["channel_details"].find_one({"channel_info.Channel_Id": channel_id})
    if existing_document:
        return "Document already exists for this channel_id"
    
    channel_det=get_channel_det(youtube,channel_id)
    playlist_det=get_playlist_det(channel_id)
    video_ids=get_all_video_ids(youtube, channel_id)
    video_det=get_video_details(youtube, video_ids)
    commend_det=get_commend_details(youtube, video_ids)
    
    collection1 =mydb["channel_details"]
    
    collection1.insert_one({"channel_info":channel_det,
                            "playlist_info":playlist_det,
                            "video_info":video_det,
                            "comment_info":commend_det
                            })
    return "uploaded successfully"

#CONNECT TO MYSQL
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Priyanka@123')
cur = myconnection.cursor()
#Create Database
cur.execute(f"create database if not exists Youtube_pro")


#CREAT CHANNELS TABLE
def channels_table():

    #To select Database
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Priyanka@123',database ="youtube_pro")
    cur = myconnection.cursor()

    #Creat Table in Mysql
    sql_create_table = f"""CREATE TABLE IF NOT EXISTS Channels (Channel_Name varchar(50),
                                                                Channel_Id varchar(50)PRIMARY KEY,
                                                                Subscription_Count int,
                                                                Channel_Views int,
                                                                Channel_Description text,
                                                                Total_Videos int,
                                                                Playlist_id varchar(100));"""
    cur.execute(sql_create_table)

    import pandas as pd
    #Creat Dataframe for Upload to mysql
    mydb = client["Youtube_Data"] 
    collection1 =mydb["channel_details"]
    Channel_inf=[]
    for i in collection1.find({},{'_id':0,"channel_info":1}):
        Channel_inf.append(i["channel_info"])
    df=pd.DataFrame(Channel_inf)

    #insert Dataframe
    sql= '''insert into Channels (Channel_Name,
                                    Channel_Id,
                                    Subscription_Count,
                                    Channel_Views,
                                    Channel_Description,
                                    Total_Videos,
                                    Playlist_id)
                                    values (%s,%s,%s,%s,%s,%s,%s)
                                    on duplicate key update Channel_Name= values(Channel_Name),
                                    Channel_Id=values(Channel_Id),
                                    Playlist_id=values(Playlist_id)'''
    for i in range(0,len(df)):
        cur.execute(sql,tuple(df.iloc[i]))
        myconnection.commit()


#CREAT PLAYLIST TABLE
def playlist_table():
    #To select Database
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Priyanka@123',database ="youtube_pro")
    cur = myconnection.cursor()

    drop_query="drop table if exists Playlist"
    cur.execute(drop_query)
    myconnection.commit()

    #Creat Table in Mysql
    sql_create_table = f"""CREATE TABLE IF NOT EXISTS Playlist (Playlist_id varchar(50)PRIMARY KEY,
                                                                Playlist_Name varchar(100),
                                                                Channel_id varchar(50),
                                                                Channel_Name varchar(100),
                                                                PublishedAt timestamp,
                                                                Video_count int);"""
    cur.execute(sql_create_table)

    #Creat Dataframe for uploading to mysql
    mydb = client["Youtube_Data"] 
    collection1 =mydb["channel_details"]
    playlist_inf=[]
    for i in collection1.find({},{'_id':0,"playlist_info":1}):
        for j in range(len(i["playlist_info"])):
            playlist_inf.append(i["playlist_info"][j])
            
    df1=pd.DataFrame(playlist_inf)

    #insert Dataframe
    sql= '''insert into Playlist (Playlist_id,Playlist_Name,Channel_id,Channel_Name,PublishedAt,Video_count)
                                    values (%s,%s,%s,%s,%s,%s)
                                    on duplicate key update Playlist_id= values(Playlist_id),
                                    Playlist_Name=values(Playlist_Name),
                                    Channel_id=values(Channel_id)'''
    df1['PublishedAt'] = pd.to_datetime(df1['PublishedAt'])
    for i in range(0,len(df1)):
        cur.execute(sql,tuple(df1.iloc[i]))
        myconnection.commit()


#CREAT VIDEOS TABLE
def videos_table():
    #To select Database
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Priyanka@123',database ="youtube_pro")
    cur = myconnection.cursor()

    drop_query="drop table if exists Videos"
    cur.execute(drop_query)
    myconnection.commit()

    #Creat Table in Mysql
    sql_create_table = """
        CREATE TABLE IF NOT EXISTS Videos (
            Channel_Name varchar(100),
            Channel_Id varchar(100),
            Video_id varchar(50) primary key,
            Video_Name varchar(150),
            video_des text,
            Tags text,
            Published_at timestamp,
            View_count bigint,
            Like_count bigint,
            Fav_count int,
            Comment_count int,
            Duration Time,
            Thum_nail varchar(200),
            Caption_status varchar(100)
        )
    """
    cur.execute(sql_create_table)

    #Creat Dataframe for uploading to mysql
    mydb = client["Youtube_Data"] 
    collection2 =mydb["channel_details"]
    Video_inf=[]
    for i in collection2.find({},{'_id':0,"video_info":1}):
        for j in range(len(i["video_info"])):
            Video_inf.append(i["video_info"][j])
        
            
    df2=pd.DataFrame(Video_inf)

    # Insert Dataframe
    for index, row in df2.iterrows():
        sql = '''INSERT INTO videos (
                    Channel_Name,
                    Channel_Id,
                    Video_id,
                    Video_Name,
                    video_des,
                    Tags,
                    Published_at,
                    View_count,
                    Like_count,
                    Fav_count,
                    Comment_count,
                    Duration,
                    Thum_nail,
                    Caption_status
                ) 
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_id'],
            row['Video_Name'],
            row['video_des'],
            row['Tags'],
            row['Published_at'],
            row['View_count'],
            row['Like_count'],
            row['Fav_count'],
            row['Comment_count'],
            row['Duration'],
            row['Thum_nail'],
            row['Caption_status']
        )

        
        cur.execute(sql,values)
        myconnection.commit()


#CREATE COMMENT TABLE
def comments_table():
    #To select Database
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Priyanka@123',database ="youtube_pro")
    cur = myconnection.cursor()

    drop_query="drop table if exists Comments"
    cur.execute(drop_query)
    myconnection.commit()

    #Creat Table in Mysql# Comments
    sql_create_table = """
        CREATE TABLE IF NOT EXISTS Comments (
            Comment_Id varchar(100) primary key,
            video_id varchar(50),
            Comment_Text text,
            Comment_Author varchar(50),
            Com_publishedDate timestamp
            
        )
    """
    cur.execute(sql_create_table)

    #Creat Dataframe for uploading to mysql
    mydb = client["Youtube_Data"] 
    collection =mydb["channel_details"]
    Comment_inf=[]
    for i in collection.find({},{'_id':0,"comment_info":1}):
        for j in range(len(i["comment_info"])):
            Comment_inf.append(i["comment_info"][j])
        
            
    df3=pd.DataFrame(Comment_inf)

    # Insert Dataframe
    for index, row in df3.iterrows():
        sql = '''INSERT INTO Comments (
                    Comment_Id,
                    video_id,
                    Comment_Text,
                    Comment_Author,
                    Com_publishedDate
                ) 
                VALUES(%s,%s,%s,%s,%s)'''

        values = (
            row['Comment_Id'],
            row['video_id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Com_publishedDate']
        )

            
        cur.execute(sql,values)
        myconnection.commit()


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully "
    

def View_channels_table():
    mydb = client["Youtube_Data"] 
    collection1 =mydb["channel_details"]
    Channel_inf=[]
    for i in collection1.find({},{'_id':0,"channel_info":1}):
        Channel_inf.append(i["channel_info"])
    df=st.dataframe(Channel_inf)

    return df


def View_playlist_table():

    mydb = client["Youtube_Data"] 
    collection1 =mydb["channel_details"]
    playlist_inf=[]
    for i in collection1.find({},{'_id':0,"playlist_info":1}):
        for j in range(len(i["playlist_info"])):
            playlist_inf.append(i["playlist_info"][j])
    df1=st.dataframe(playlist_inf)

    return df1


def View_videos_table():
    mydb = client["Youtube_Data"] 
    collection2 =mydb["channel_details"]
    Video_inf=[]
    for i in collection2.find({},{'_id':0,"video_info":1}):
        for j in range(len(i["video_info"])):
            Video_inf.append(i["video_info"][j])
    df2=st.dataframe(Video_inf)   

    return df2    


def View_comments_table():

    mydb = client["Youtube_Data"] 
    collection =mydb["channel_details"]
    Comment_inf=[]
    for i in collection.find({},{'_id':0,"comment_info":1}):
        for j in range(len(i["comment_info"])):
            Comment_inf.append(i["comment_info"][j])
    df3=st.dataframe(Comment_inf)

    return df3



#streamlit

import streamlit as st
with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Home")
    st.write("Lets Explore Some details of your favorite Youtube Channels.")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    channel_ids=[]
    mydb = client["Youtube_Data"] 
    collection1 = mydb["channel_details"]
    
    # Check if the channel ID already exists in the collection
    existing_channel = collection1.find_one({"channel_info.Channel_Id": channel_id})
    
    if existing_channel:
        st.success("Given Channel Id Details Already Exist")
    else:
        insert = channel_details(channel_id)
        st.success(insert)


if st.button("Migrated to SQL"):
    Table=tables()
    st.success(Table)

show_tables= st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLIST","VIDEOS","COMMENT"))

if show_tables=="CHANNELS":
    View_channels_table()

elif show_tables=="PLAYLIST":
    View_playlist_table()

elif show_tables=="VIDEOS":
    View_videos_table()

elif show_tables=="COMMENT":
    View_comments_table()



#Connect to SQL
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Priyanka@123',database ="youtube_pro")
cur = myconnection.cursor()

Query= st.selectbox("SELECT YOUR QUERY",("1. Channel Name and their Videos",
                                         "2. Channel with Highest Num of Videos",
                                         "3. Top 10 Viewed Videos",
                                         "4. Comments in each Videos",
                                         "5. Videos with highest like",
                                         "6. Like count of each videos",
                                         "7. Total num of View for Each Channels",
                                         "8. In 2022 how many videos Published in each Channels",
                                         "9. Average duration of All videos in each Channels",
                                         "10. Videos which is Highest Num of Comments and their Channel Names"))

myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='Priyanka@123',database ="youtube_pro")
cur = myconnection.cursor()

if Query=="1. Channel Name and their Videos":
    query1='''select Channel_Name as Channelname, Video_Name as videos from videos'''
    cur.execute(query1)
    myconnection.commit()
    tab1=cur.fetchall()
    df=pd.DataFrame(tab1,columns=["Channel Name","Video Title"])
    st.write(df)


elif Query=="2. Channel with Highest Num of Videos":
    query2='''select Channel_Name as ChannelName, Total_Videos as Num_Videos 
                from channels order by Total_Videos desc'''
    cur.execute(query2)
    myconnection.commit()
    tab2=cur.fetchall()
    df2=pd.DataFrame(tab2,columns=["Channel Name", "Total Videos"])
    st.write(df2)


elif Query=="3. Top 10 Viewed Videos":
    query3='''select Video_Name as VideoTitle,Channel_Name as ChannelName,View_count as Views 
                from videos 
                where View_count is not null order by View_count desc limit 10 '''
    cur.execute(query3)
    myconnection.commit()
    tab3=cur.fetchall()
    df3=pd.DataFrame(tab3,columns=["Video Title","Channel Name","Views"])
    st.write(df3)


elif Query=="4. Comments in each Videos":
    query4='''select Video_Name as VideoTitle,Channel_Name as ChannelName, Comment_count as Num_Count 
                from videos where Comment_count is not null '''
    cur.execute(query4)
    myconnection.commit()
    tab4=cur.fetchall()
    df4=pd.DataFrame(tab4,columns=["Video Title","Channel Name","Comment Count"])
    st.write(df4)


elif Query=="5. Videos with highest like":
    query5='''select Video_Name as VideoTitle,Channel_Name as ChannelName, Like_count as Like_Count 
                from videos where Like_count is not null order by Like_count desc limit 10'''
    cur.execute(query5)
    myconnection.commit()
    tab5=cur.fetchall()
    df5=pd.DataFrame(tab5,columns=["Video Title","Channel Name","Like Count"])
    st.write(df5)


elif Query=="6. Like count of each videos":
    query6='''select Video_Name as VideoTitle,Channel_Name as ChannelName, Like_count as Like_Count 
                from videos'''
    cur.execute(query6)
    myconnection.commit()
    tab6=cur.fetchall()
    df6=pd.DataFrame(tab6,columns=["Video Title","Channel Name","Like Count"])
    st.write(df6)


elif Query=="7. Total num of View for Each Channels":
    query7='''select Channel_Name as ChannelName, Channel_Views as Total_Views from channels'''
    cur.execute(query7)
    myconnection.commit()
    tab7=cur.fetchall()
    df7=pd.DataFrame(tab7,columns=["Channel Name","Views Count"])
    st.write(df7)


elif Query=="8. In 2022 how many videos Published in each Channels":
    query8='''select Channel_Name as ChannelName,Video_Name as VideoTitle, Published_at as ReleaseDate 
                from videos 
                where extract(year from Published_at)=2022'''
    cur.execute(query8)
    myconnection.commit()
    tab8=cur.fetchall()
    df8=pd.DataFrame(tab8,columns=["Channel Name","Video Title","Release Date"])
    st.write(df8)


elif Query=="9. Average duration of All videos in each Channels":
    query9='''select Channel_Name as ChannelName,avg(Duration) as AverageDuration 
                from videos group by Channel_Name'''
    cur.execute(query9)
    myconnection.commit()
    tab9=cur.fetchall()
    df9=pd.DataFrame(tab9,columns=["Channel Name","Average Video Duration"])
    st.write(df9)


elif Query=="10. Videos which is Highest Num of Comments and their Channel Names":
    query10='''select Channel_Name as ChannelName,Video_Name as VideoName, Comment_count as CommentCount 
                from videos where Comment_count is not null order by Comment_count desc limit 10'''
    cur.execute(query10)
    myconnection.commit()
    tab10=cur.fetchall()
    df10=pd.DataFrame(tab10,columns=["Channel Name","Video Name","Comment Count"])
    st.write(df10)

