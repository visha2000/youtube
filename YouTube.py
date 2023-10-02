import streamlit as st 
from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
from pymongo import MongoClient
import pymysql
from requests import request
from datetime import datetime,date
from isodate import parse_duration
from googleapiclient.errors import HttpError

st.markdown("<h1 style='color: blue; front-size: 12px';text-align:center;>YouTube Data Harvesting and Warehousing</h1>",
            unsafe_allow_html=True)

#connecting mongodb
loc=MongoClient("mongodb://localhost:27017")   
db=loc["YouTube"]
YT=db["Table_Channel"]

# connecting with pymysql database
con=pymysql.connect(
    host="localhost",
    user="root",
    password="VISHA#13@agv",
    database="project",
    port = 3306 )
mycursor=con.cursor()

# Building connection with youtubeapi
Api_key = "AIzaSyB-1DhcU8wQgFGMVrwDcilt6o6nsf4hZig"                         #"AIzaSyDj2E0NtxbgnjbjBa2x_Zjqgbr2kcJ412Y"
api_service_name = "youtube"
api_version = "v3"
youtube =build(api_service_name, api_version, developerKey = Api_key) 

# selectbox option
GETYOUTUBEDATA=st.sidebar.selectbox('[select option]',['Data collection','Select and Store','Migrate Data','Query Data'])
if GETYOUTUBEDATA == "Data collection":
    st.title(f"DATA COLLECTION PAGE")
    DF=pd.DataFrame(
        {'ChannelNames':['paridhabangal','streamlit','foodzee','village cooking channel','Nakkalites FZone','ICC','TAMIL VOICE OVER','see saw','Magneq software','Techqflow software solutions '],
         'ChannelIds':["UCueYcgdqos0_PzNOq81zAFg","UC3LD42rjj-Owtxsa6PwGU5Q","UCSQZKcpAktwOv6S-S-KvSoA","UCk3JZr7eS3pg5AGEvBdEvFg","UCpnJuQkNm9j9R7LCqWtf56g","UCt2JXOLNxqry7B_4rRZME3Q","UCTIuWYnWo-7CmYZqXD8WFRA","UCMyi2FZAUNLFDUE8ZIf7YOQ","UCqkqhves7gxkdWBAAvRWPKw","UCPr2A4dx8qfMcKDE0coBl8A"]
         })
    st.dataframe(DF)

#Function to get channel_details
def get_channel_details(youtube,channel_Ids):
    channel_data=[]
    request = youtube.channels().list(
      part = "snippet,contentDetails,statistics",
      id=channel_Ids
      )
    response = request.execute()

    for item in response['items']:
        data={'channel_id':item["id"],
              'channelname':item["snippet"]["title"],
              'subscriber':item["statistics"]["subscriberCount"],
              'video':item["statistics"]["videoCount"],
              'view':item["statistics"]["viewCount"],
              'description':item["snippet"]["description"],
              'playlist_id':item["contentDetails"]["relatedPlaylists"]["uploads"],
              'publishedAt':item["snippet"]["publishedAt"],
         }
        channel_data.append(data)

    return channel_data

# Function to get playlist_details
def get_playlist_details(youtube,channel_Ids):
    playlist_data=[]
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_Ids,
        maxResults=50
        )
    response = request.execute()
    
    for i in range(len(response["items"])):
        data1=dict(channel_id=response['items'][i]["snippet"]["channelId"],
                channel_title=response['items'][i]["snippet"]['channelTitle'],
                playlistId=response['items'][i]["id"],
                playlist_title=response['items'][i]['snippet']['localized']['title'],
                description=response['items'][i]["snippet"]['localized']['description'],
                publishedAt=response['items'][i]["snippet"]['publishedAt'],
                ItemCount=int(response['items'][i]['contentDetails']['itemCount'])

                )
        playlist_data.append(data1)
        next_page_token=response.get('nextPageToken')
        more_pages=True

        while more_pages:
            if next_page_token is None:
                more_pages=False
        else:
             request = youtube.playlists().list(
               part="snippet,contentDetails",
               channelId=channel_Ids,
               maxResults=50
               )
        response = request.execute()
        for i in range(len(response["items"])):
            data1=dict(channel_id=response['items'][i]["snippet"]["channelId"],
                channel_title=response['items'][i]["snippet"]['channelTitle'],
                playlistId=response['items'][i]["id"],
                playlist_title=response['items'][i]['snippet']['localized']['title'],
                description=response['items'][i]["snippet"]['localized']['description'],
                publishedAt=response['items'][i]["snippet"]['publishedAt'],
                ItemCount=int(response['items'][i]['contentDetails']['itemCount'])
                )
            playlist_data.append(data1)
        next_page_token=response.get('nextPageToken')

    return playlist_data 

# Function to get video_ids
def get_video_ids(channel_Ids):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_Ids, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

# Function to get video_details
def get_video_details(youtube, video_ids):
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids[i:i + 50])
        )
        response = request.execute()

        for video in response['items']:
            duration_str = video['contentDetails']['duration']
            duration = parse_duration(duration_str)

            # Extract hours, minutes, and seconds from timedelta
            total_seconds = int(duration.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60

            # Use datetime.now() to get the current datetime
            published_datetime = datetime.now()
            video_stats = dict(
                channelId=video['snippet']['channelId'],
                channelname=video['snippet']['channelTitle'],
                video_ids=video['id'],
                Title=video['snippet']['title'],
                publishedAt=video['snippet']['publishedAt'],
                published_datetime=published_datetime,
                published_date=str(published_datetime.date()),
                 viewCount=int(video['statistics'].get('viewCount',0)),
                likeCount=int(video['statistics'].get('likeCount', 0)),
                commentCount=int(video['statistics'].get('commentCount', 0)),
                favoriteCount=int(video['statistics'].get('favoriteCount', 0)),
                duration=duration_str,  # Keep the original duration string
                Duration="{}:{}:{}".format(hours, minutes, seconds),  # Human-readable duration
                caption=video['contentDetails']['caption'],
                definition=video['contentDetails']['definition'],
                dimension=video['contentDetails']['dimension'],
                projection=video['contentDetails']['projection'],
                categoryId=video['snippet']['categoryId']
            )
            all_video_info.append(video_stats)

    return all_video_info

# Function to get comment_details
def get_comments_in_video(youtube,video_ids):
    all_comments=[]

    for i in video_ids:
        try:

            request=youtube.commentThreads().list(
              part="snippet,replies",
              videoId=i,
              maxResults=50,
            )
            response=request.execute()
            for comment in response['items']:
                data2={'channelId':comment['snippet']['channelId'],
                       'commentId':comment['snippet']['topLevelComment']['id'],
                       'comment_Text':comment['snippet']['topLevelComment']['snippet']['textOriginal'],
                       'comment_Author':comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                       'comment_publishedAt':comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                       'Like_count':comment['snippet']['topLevelComment']['snippet']["likeCount"],
                       'comment_Reply':comment['snippet'] ["canReply"],
                       'totalReplyCount':comment['snippet']['totalReplyCount'],
                       'videoId':comment['snippet']['videoId'],
                      }

            all_comments.append(data2)
        except:
            pass
    return all_comments

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.Table_Channel.find():
        ch_name.append(i['channel details'])
    return ch_name
#DF1=pd.DataFrame(channel_names)
#DF1.to_sql(name='channel_table', sql_con=con,if_exists='replace',index=False)
#st.success(f"channel_names: {selected_channel}")

#connecting mongodb
if GETYOUTUBEDATA == "Select and Store" :
    channel_Ids=st.text_input("Enter the channel id")
    if channel_Ids and st.button('store in mongodb'):     
            # Main Function
            def main(channel_Ids):
                c=get_channel_details(youtube,channel_Ids)
                p=get_playlist_details(youtube,channel_Ids)
                vi=video_ids=get_video_ids(channel_Ids)    
                v=get_video_details(youtube,video_ids)
                cm=get_comments_in_video(youtube,video_ids)

                data={'channel details':c,
                    'playlist details':p,
                    'videoid details':vi,
                    'video details':v,
                    'comment details':cm}
                return data
            data=main(channel_Ids)
            YT.insert_one(data)
            st.write(data)
            st.success("Upload to MongoDB successful !!")
#for i in YT.find({},{'id':0}):
    #pprint(i)
#i.keys()

# Migrate the data mongodb to sql
if GETYOUTUBEDATA == "Migrate Data" :
    st.markdown("#   ")
    st.markdown("### Select a channel to begin mongodb to SQL")  #channel_Ids=st.text_input("Enter the channel id")
    ch_names = channel_names()  
    user_inp = st.selectbox("Select channel",options=ch_names)
    #option= st.selectbox(options=ch_names)
    #st.write('you selected:',option)
    for i in YT.find({},{'id':0}):
       pprint(i)
    i.keys()

    def insert_into_channel():
    # Assuming 'i' is a MongoDB document
        channel_details_list = i.get('channel details')

        if channel_details_list and isinstance(channel_details_list, list):
            sql="""INSERT INTO channel_table(channel_id,channelname,subscriber,video,view,description,playlist_id,publishedAt)
                  VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
            for channel_details in channel_details_list:
                # Assuming channel_details is a dictionary
                values = tuple(channel_details.values())

                mycursor.execute(sql, values)
                con.commit()

    def insert_into_playlist():
         playlist_details_list = i.get('playlist details')
         if playlist_details_list and isinstance(playlist_details_list, list):
            sql1="""INSERT INTO playlist_table(channel_id,channel_title,playlistId,playlist_title,description,publishedAt,ItemCount)
                VALUES(%s,%s,%s,%s,%s,%s,%s)"""
            for playlist_details in playlist_details_list:
                # Assuming playlist_details is a dictionary
                value = tuple(playlist_details.values())

                mycursor.execute(sql1, value)
                con.commit()

    def insert_into_video():
        video_details_list = i.get('video details')
        if video_details_list and isinstance(video_details_list, list):
            sql2="""INSERT INTO video_table(channelId,channelname, video_ids,Title,publishedAt,published_datetime,published_date,
                        viewCount,likeCount,commentCount,
                        favoriteCount,duration,Duration_time,caption,definition,dimension,projection,categoryId)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            for video_details in video_details_list:
                # Assuming video_details is a dictionary
                values = tuple(video_details.values())

                mycursor.execute(sql2, values)
                con.commit()
        
    def insert_into_comment():
         comment_details_list = i.get('comment details')
         if comment_details_list and isinstance(comment_details_list, list):
            sql3="""INSERT INTO comment_table(channelId,commentId, comment_Text,comment_Author,comment_publishedAt,like_count,comment_Reply,
                totalReplyCount,videoId)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            for comment_details in comment_details_list:
                # Assuming comment_details is a dictionary
                values = tuple(comment_details.values())

                mycursor.execute(sql3, values)
                con.commit()
        

    if st.button("Submit"):
            try:
                insert_into_channel()
                insert_into_playlist()
                insert_into_video()
                insert_into_comment()
                st.success("Transformation Mongodb to MySQL Successful !!")
            except:
                st.error("Channel details already transformed !!")

#Query Data
if GETYOUTUBEDATA == "Query Data":
    question_tosql = st.selectbox("**select your question**",
    ('1.What are the names of all the videos and their corresponding channels?',
    '2.Which channels have the most number of videos, and how many videos do they have?',
    '3.What are the top 10 most viewed videos and their respective channels?',
    '4.How many comments were made on each video, and what are their corresponding video names?',
    '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7.What is the total number of views for each channel, and what are their corresponding channel names?',
    '8.What are the names of all the channels that have published videos in the year 2022?',
    '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))

    if question_tosql == '1.What are the names of all the videos and their corresponding channels?':
            query1="select v.Title,c.channelname from video_table v join channel_table c ON v.channelId=c.channel_id"
            mycursor.execute(query1)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df1=pd.DataFrame(data1,columns=["Title","channelname"])
                st.dataframe(df1)
            else:
                st.write("No result found.")

    elif question_tosql == "2.Which channels have the most number of videos, and how many videos do they have?":
            query2 = "select channelname,video from channel_table order by video desc limit 1"
            mycursor.execute(query2)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df2=pd.DataFrame(data1,columns=["channelname","video"])
                st.dataframe(df2)
            else:
                st.write("No result found.")

    elif question_tosql == '3.What are the top 10 most viewed videos and their respective channels?':
            query3 = "select c.channelname,v.video_ids,v.viewCount from channel_table c join video_table v ON c.channel_id=v.channelId order by v.viewCount desc limit 10"
            mycursor.execute(query3)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df3=pd.DataFrame(data1,columns=["channelname","video_ids","viewCount"])
                st.dataframe(df3)
            else:
                st.write("No result found.")

    elif question_tosql == '4.How many comments were made on each video, and what are their corresponding video names?':
            query4 = "select Title,commentCount from video_table"
            mycursor.execute(query4)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df4=pd.DataFrame(data1,columns=["Title","commentCount"])
                st.dataframe(df4)
            else:
                st.write("No result found.")

    elif question_tosql == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
            query5 = "select v.likeCount,c.channelname from video_table v left join channel_table c ON v.channelId=c.channel_id ORDER BY likeCount  desc limit 10 "
            mycursor.execute(query5)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df5=pd.DataFrame(data1,columns=["likeCount","channelname"])
                st.dataframe(df5)
            else:
                st.write("No result found.")

    elif question_tosql == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            query6 = "select Title,likeCount from video_table"
            mycursor.execute(query6)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df6=pd.DataFrame(data1,columns=["Title","likeCount"])
                st.dataframe(df6)
            else:
                st.write("No result found.")
    
    elif question_tosql == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
            query7 = "select view,channelname from channel_table"
            mycursor.execute(query7)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df7=pd.DataFrame(data1,columns=["view","channelname"])
                st.dataframe(df7)
            else:
                st.write("No result found.")

    elif question_tosql == '8.What are the names of all the channels that have published videos in the year 2022?':
            query8 = "select c.channelname,v.video_ids,v.published_date from channel_table c join video_table v  ON c.channel_id=v.channelId where year(v.published_date)='2023'"
            mycursor.execute(query8)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df8=pd.DataFrame(data1,columns=["channelname","video_ids","publishedAt"])
                st.dataframe(df8)
            else:
                st.write("No result found.")

    elif question_tosql == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            query9 = "select c.channelname,avg(v.Duration_time) from channel_table c join video_table v ON c.channel_id=v.channelId GROUP BY c.channelname"
            mycursor.execute(query9)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df9=pd.DataFrame(data1,columns=["channelname","duration"])
                st.dataframe(df9)
            else:
                st.write("No result found.")

    elif question_tosql == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
            query10 = "select v.commentCount,c.channelname from video_table v join channel_table c order by commentCount desc limit 10"
            mycursor.execute(query10)
            data1=[i for i in mycursor.fetchall()]
            if data1:
                st.write("Results:")
                df10=pd.DataFrame(data1,columns=["commentCount","channelname"])
                st.dataframe(df10)
            else:
                st.write("No result found.")

                    











































