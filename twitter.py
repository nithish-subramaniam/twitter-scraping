#importing the modules which are al required
import snscrape.modules.twitter as sntwitter
import pandas as pd
import streamlit as st
import pymongo

#Initiallzation of Mongodb server and creating database and collection
def mongo(df):    
    client = pymongo.MongoClient("mongodb://localhost:27017") 
    mydb = client["twitter_database"]   
    mycoll = mydb[f"{search}_collection"] 
    df.reset_index(inplace=True)
    data_dict = df.to_dict("records")
    mycoll.insert_one({"index":f"{search}","data":data_dict})
    st.success("Uploaded Successfully!",icon='✅') 
    collections = mydb.list_collection_names()
    st.write("collection already exists : ")
    st.write(collections)

#defining function to scrape the data from twitter

tweets_list1 = []
with st.form("my_form"):
    default_since = '2020-01-01'
    default_until = '2023-01-31'
    search = st.text_input("Enter key word to search : ")
    since = st.text_input('Enter the start_date :',default_since) 
    until = st.text_input('Enter the end_date :', default_until)
    maxTweets = st.slider('Enter the tweet count to scrape:', 0,1000,100)
    maxTweets = int(maxTweets)
    summit = st.form_submit_button('Submit')
    if summit:
        allsearch = (f'{search} since:{since} until:{until}')
        for i,tweet in enumerate(sntwitter.TwitterSearchScraper(allsearch).get_items()):
            if i>maxTweets:
                break
            tweets_list1.append([tweet.date, tweet.id, tweet.content, tweet.user.username, tweet.url, tweet.replyCount,  tweet.retweetCount,tweet.lang, tweet.likeCount ])


tweets_df1 = pd.DataFrame(tweets_list1, columns=['DateTime', 'Tweet_ID', 'Content', 'User_Name', 'URL', 'Reply_count', 'Re_Tweet_Count','Language', 'Like_Count'])
st.write(tweets_df1)

#creating the button to upload the data into mongodb server
with st.form("form"):
    st.write("Press Enter to upload dataset into DB : ")
    enter = st.form_submit_button("Enter")
    if enter:
        mongo(tweets_df1)
        
#defining the funtion to conver the dataset into csv format        
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

csv = convert_df(tweets_df1)

st.download_button(
"Press to Download the Dataframe to CSV file format",
csv,
f"{search}_tweet.csv",
"text/csv",
key='download-csv'
)

#defining the funtion to conver the dataset into json format
def convert_json(df):
    return df.to_json().encode('utf-8')

json = convert_json(tweets_df1)
st.download_button(
"Press to Download the Dataframe to JSON file format",
json,
f"{search}_tweet.json",
"text/json",
key='download-json'
)
