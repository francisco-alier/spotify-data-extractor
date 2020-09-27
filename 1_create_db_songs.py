#import packages
import pandas as pd 
import spotipy
import os

#Set current working directory to your computer Desktop

#create the spotify object
sp = spotipy.Spotify() 

#import credentials
from spotipy.oauth2 import SpotifyClientCredentials 

#input user_id and user_secret
cid ="" 
secret = ""

client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)

#Request access
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager) 
sp.trace=False 


#Get Metal playlist
uris = []
pops = []
dates = []
bands = []
songs = []
explicits = []

def get_data_from_playlist(genre):
    
    df_final = {}
    play = sp.search(q =  genre, limit=1, offset=0, type='playlist', market=None)
    
    #iterate playlists
    for i in range(len(play['playlists']['items'])):
        
        
        playlist_id = play['playlists']['items'][i]['id']
    
        playlist = sp.playlist_tracks(playlist_id)
    

        # second loop to generate data
        for j in range(len(playlist['items'])):

            uri = playlist['items'][j]['track']['uri'] 
            pop = playlist['items'][j]['track']['popularity'] 
            date = playlist['items'][j]['track']['album']['release_date']
            band = playlist['items'][j]['track']['artists'][0]['name']
            song = playlist['items'][j]['track']['name']
            explicit = playlist['items'][j]['track']['explicit']
            
            uris.append(uri)
            pops.append(pop)
            dates.append(date)
            bands.append(band)
            songs.append(song)
            explicits.append(explicit)
            
            features = sp.audio_features(uris)
            
            df = pd.DataFrame(features)
            
            df = df.join(pd.DataFrame(
                    {
                    'artist': bands,
                    'name_song': songs,
                    'popularity': pops,
                    'release_date': dates,
                    'is_explicit': explicits,
                    'genre': genre
                    }, index = df.index
                ))
            
        pd.DataFrame(df_final).append(df)

        print(f'Playlist number {i} extracted!')
    
    return df_final

data_metal = get_data_from_playlist(genre = 'metal')
#data_blues = get_data_from_playlist(genre = 'blues')
#data_pop = get_data_from_playlist(genre = 'pop')
#data_rock = get_data_from_playlist(genre = 'rock')
