import pandas as pd
import numpy as np
import json
import os
import ast
from geopy import distance
import googlemaps

from geopy.exc import (
    GeocoderQueryError,
    GeocoderQuotaExceeded,
    ConfigurationError,
    GeocoderParseError,
    GeocoderTimedOut
)

from config import *




def get_travel_dist(gmaps, P_node, C_node, retry_counter=1):
    '''
    apply gmaps Google Distance matrix API to find the nearest one with shortest travel distance
    Inputs: P_node: target patient node
            C_node: Client node
    Output: dist: shortest travel distance
    '''
    
    try:
        # the gmaps api call
        dist = gmaps.distance_matrix(P_node, C_node, mode='driving')
        
        if dist is not None:
            # locate the distance value from returned string
            dist_result = dist["rows"][0]["elements"][0]["distance"]["value"]/1000
        else:
            #if API server fails all, travel distances will set to the same default value
            # later the one with shorter geodesic distance will be picked
            dist_result = DEFAULT_DIST
     
    # To catch generic geocoder errors.
    except (ValueError, GeocoderQuotaExceeded, ConfigurationError, GeocoderParseError) as error:
        if hasattr(error, 'message'):
            error_message = error.message
        else:
            error_message = error
        print(error_message)
        dist_result = DEFAULT_DIST
    # To retry because intermittent failures and timeout sometimes occurs
    except (GeocoderTimedOut, GeocoderQueryError) as geocodingerror:
        if retry_counter < RETRY_COUNTER_CONST:
            print(f'Retrying {retry_counter} time(s)' )
            return get_travel_dist(P_node, C_node, retry_counter+1)
        else:
            if hasattr(geocodingerror, 'message'):
                error_message = geocodingerror.message
            else:
                error_message = geocodingerror
            print(error_message)
            dist_result = DEFAULT_DIST
    # To retry because intermittent failures and timeout sometimes occurs
    except BaseException as error:
        if retry_counter < RETRY_COUNTER_CONST:
#             time.sleep(5)
            print(f'Retrying {retry_counter} time(s)')
            return get_travel_dist(P_node, C_node, retry_counter+1)
        else:
            print(error)
            dist_result = DEFAULT_DIST
    
    return dist_result



def map_files(P_df, C_df, output_path):
    
    '''
    first using geodesic to filter out N nearest locations
    then apply google distance matrix to compute the travel distance
    '''
    # cols mapping
    PC_mapped = pd.DataFrame(columns = OUTPUT_COLS)
    PC_mapped["Patient_ID"] = P_df["ID"]
    PC_mapped["Pat_Geo_Cols"] = P_df["Pat_Geo_Cols"]
    PC_mapped["Pat_Geocode"] = P_df["Pat_Geocode"]
    PC_mapped["Pat_Address"] = P_df["Address"]
    PC_mapped["Pat_Postal_Code"] = P_df["Postal Code"]
    PC_mapped["Pat_FSA"] = P_df["FSA"]
    
    # initialize the distance API
    gmaps = googlemaps.Client(key=API_KEY)
    
    for i, p_r, in PC_mapped.iterrows():
        # target patient node
        P_node = p_r['Pat_Geocode']
        
        # target pool of N locations with neareset geodesic distance
        C_df['dist'] = C_df['Clinic_Geocode'].apply(lambda p : distance.geodesic(P_node, p).km)
        N_pool = C_df.nsmallest(N, 'dist', keep='all').reset_index()
        
        N_pool['travel_dist'] = ''
        
        # function that call the Google distance matrix API
        for n_i, r in N_pool.iterrows():
            N_pool.at[n_i,'travel_dist'] = get_travel_dist(gmaps, P_node, r["Clinic_Geocode"])
        
        # pick the one with smallest travel_dist, in case of duplicate, keep the first 
        # thus the one with shorter geodesic distance will be picked
        N_pool['travel_dist'] = N_pool['travel_dist'].apply(np.float)
        Nearest = N_pool.nsmallest(1, 'travel_dist', keep='first')
        
        PC_mapped.at[i, "Nearest_Clinic_ID"] = Nearest["Clinic ID"].values[0]
        PC_mapped.at[i, "Clinic_Geo_Cols"] = Nearest["Clinic_Geo_Cols"].values[0]
        PC_mapped.at[i, "Clinic_Geocode"] = Nearest["Clinic_Geocode"].values[0]
        PC_mapped.at[i, "Clinic_Address"] = Nearest["Clinic Address"].values[0]
        PC_mapped.at[i, "Clinic_Postal_Code"] = Nearest["Postal Code"].values[0]
        PC_mapped.at[i, "Clinic_FSA"] = Nearest["FSA"].values[0]
        PC_mapped.at[i, "Clinic_Distance"] = Nearest["travel_dist"].values[0]
        
    # output final mapped dataframes
    try:
        if not os.path.isdir(output_path):
            os.makedirs(output_path)
        PC_mapped.to_csv(os.path.join(output_path, 'PC_mapped.csv'), index=False)
    except:
        print("Fail to output final Mapped dataframes, please check the output path is corret")
    
    return PC_mapped