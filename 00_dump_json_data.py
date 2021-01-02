

# Import the module
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
ids = []
explicits = []
pops = []

for idx, name in enumerate(unique_tracks):
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
    
get_features(track_id='7dzwCwvobdoKlIHr9XPZns', cid=cid, secret=secret)
