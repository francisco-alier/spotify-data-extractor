

# Import the modules
import json
import os
import pandas as pd

os.getcwd()
# First function to get streaming history

'''##############   PHASE 1 - streaming history #####################'''

def get_streamings(path_data: str = r'data/'):
    
    streamings = [file for file in os.listdir(path_data) if file.startswith('StreamingHistory')]

    all_streamings = []
    
    for stream_file in streamings:
        
        with open(path_data + stream_file, 'r', encoding="utf8") as file:
            data = json.load(file)
            
            all_streamings += [streaming for streaming in data]
            
            
    df = pd.DataFrame(all_streamings)
    
    return df

data = get_streamings()

'''##############   PHASE 2 - track ids, explicits and popularity measures #####################'''

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials 

cid = ""
secret = ""

def get_song_info(song_name: str, cid: str, secret: str, type_info: str = 'id'):
    #create the spotify object
    sp = spotipy.Spotify() 
    
    #input user_id and user_secret
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    
    #Request access
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager) 
    sp.trace=False
    
    if type_info not in ['id', 'explicit', 'popularity']:
        print('ERROR - please input one of id, explicit or popularity!')
        
    else:
        search = sp.search(q =  song_name, limit = 1, offset=0, type='track', market=None)
        info = search['tracks']['items'][0][type_info]
        
    return info

unique_tracks = data['trackName'].unique()
# 30 characters is the max a name of a song can have to return values!

search = [song[0:30] for song in unique_tracks]
ids = []
explicits = []
pops = []

for idx, name in enumerate(search):
    try:
        print(f"{len(unique_tracks) - idx} songs missing")
        id = get_song_info(song_name = name, cid = cid, secret = secret, type_info = 'id')
        explicit = get_song_info(song_name = name, cid = cid, secret = secret, type_info = 'explicit')
        pop = get_song_info(song_name = name, cid = cid, secret = secret, type_info = 'popularity')
        
        ids.append(id)
        explicits.append(explicit)
        pops.append(pop)
    except:
        print(f"{name} is a podcast?")
        ids.append('ERROR')
        explicits.append('ERROR')
        pops.append('ERROR')

unique_tracks_data = pd.DataFrame({'id': ids,
                      'name': unique_tracks,
                      'explicit': explicits,
                      'popularity': pops
                      })

'''##############   PHASE 3 - features part #####################'''

def get_features(track_id: str, cid: str, secret: str) -> dict:
    #create the spotify object
    sp = spotipy.Spotify() 
    
    #input user_id and user_secret
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, 
                                                          client_secret=secret)
    
    #Request access
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager) 
    sp.trace=False
    
    try:
        features = sp.audio_features([track_id])
        return features[0]
    except:
        return None

features_all = {}   
count = 0
for id, track in zip(ids, search):

    
    print(f"{len(search) - count} songs missing")
    features = get_features(track_id=id, cid=cid, secret=secret)
    if features:
        features_all[track] = features
        
    count = count + 1
    
df_features = pd.DataFrame(features_all).transpose().reset_index()
df_features.rename(columns={'index': 'trackName'}, inplace=True)

'''##############   PHASE 4 - Merge all data and save csv #####################'''
# remove errors
ids_final = [id for id in ids if id not in 'ERROR']
explicits_final = [exp for exp in explicits if isinstance(exp, bool)]
pops_final = [pop for pop in pops if isinstance(pop, int)]

track_no_features = ([v for v in ids_final if v not in list(df_features['id'])])

df_features_int = df_features

# join previous lists into unique dataframe
df_int = pd.DataFrame(list(zip(ids_final, explicits_final, pops_final)), 
               columns =['id', 'explicit', 'popularity'])

# Join features and intermediate by track id
df_final = pd.merge(df_features_int, df_int, how = 'inner', on = 'id')

# drop duplicates
df_final_no_dup = df_final.drop_duplicates() # perfect shape :)

# join with streaming data
df_to_export = pd.merge(data, df_final_no_dup, how = 'left', on = 'trackName')
df_to_export.reset_index(drop=True).to_csv('./data/streaming_data_clean.csv')
