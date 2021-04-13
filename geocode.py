import geopy
import ast
import sys
import os
import json
import pandas as pd
import numpy as np
from geopy.geocoders import GoogleV3
from functools import partial 

from tqdm import tqdm
tqdm.pandas()

from geopy.exc import (
    GeocoderQueryError,
    GeocoderQuotaExceeded,
    ConfigurationError,
    GeocoderParseError,
    GeocoderTimedOut
)

from config import *


def generate_address_list(r, Geo_Cols, schema):
    '''
    Generate a list of fallback address list in the order per schema
    '''
    address_list = []
    
    address = ""
    for c in Geo_Cols.split(','):
        if not address:
            address += r[c]
        else:
            address += ", " + r[c]
            
    address_list.append(address)
    
    for cc in schema:
        address_list.append(r[cc])
    
    return address_list


def geocode_address(geo_locator, address_list, retry_counter=1):
    '''
    Request GoogleV3 service for geocode data
    Exceptions are caught and handled
        - if Connection Error, retry $RETRY_COUNTER_CONST times
        - otherwise fall back to next precision level address and request the geolocation again
    Inputs: geo_locator: GoogleV3 geocoder
            address_list: a fall back address list in precision order, most precise address at position 0
            retry_counter: initial retry value
    Outputs: location_result: Geocoded lat/lng values received from the server
                            *note: if all precision level failed to retrive a valid data, return DEFAULT_LOC
            al: final state of address fallback list, according its consumed level, 
                assign the corresponding Geo_Cols info to the dataframe
    '''
    
    try:
        # default case if exhuasted the fallback list still no location found
        if not address_list:
            al = address_list
            return DEFAULT_LOC, al

        # the geopy GoogleV3 geocoding call
        location = geo_locator(address_list[0])
        
        if location is not None:
            location_result = tuple(location.point[:2])
            al = address_list
#             print(f'location : {location}, al: {al}')
        else:
            location_result, al = geocode_address(geo_locator, address_list[1:], retry_counter)

    # To catch generic geocoder errors.
    except (ValueError, GeocoderQuotaExceeded, ConfigurationError, GeocoderParseError) as error:
        if hasattr(error, 'message'):
            error_message = error.message
        else:
            error_message = error
        print(error_message)
        lcoation_result, al = geocode_address(geo_locator, address_list[1:], retry_counter)
    # To retry because intermittent failures and timeout sometimes occurs
    except (GeocoderTimedOut, GeocoderQueryError) as geocodingerror:
        if retry_counter < RETRY_COUNTER_CONST:
            print(f'Retrying {retry_counter} time(s) ' )
            return geocode_address(geo_locator, address_list, retry_counter+1)
        else:
            if hasattr(geocodingerror, 'message'):
                error_message = geocodingerror.message
            else:
                error_message = geocodingerror
            print(error_message)
            location_result, al = geocode_address(geo_locator, address_list[1:], retry_counter)
    # To retry because intermittent failures and timeout sometimes occurs
    except BaseException as error:
        if retry_counter < RETRY_COUNTER_CONST:
#             time.sleep(5)
            print(f'Retrying {retry_counter} time(s) ')
            return geocode_address(geo_locator, address_list, retry_counter+1)
        else:
            print(error)
            location_result, al = geocode_address(geo_locator, address_list[1:], retry_counter)

    return location_result, al


def geocode_files(P_df, C_df, output_path):
    '''
    Calling geocode_address function and adding corresponding result cols
    Outputs: processed Patients & Clinics dataframes
    '''
    
    geocoder = GoogleV3(api_key=API_KEY)
    geo_locator = partial(geocoder.geocode, components=COMPONENT_RESTRICTIONS)
    
    P_df["Pat_Geo_Cols"] = ",".join(map(str, P_GEO_COLS))
    C_df["Clinic_Geo_Cols"] = ",".join(map(str, C_GEO_COLS))
    
    P_df["Pat_Geocode"] = ''
    C_df["Clinic_Geocode"] = ''
    
    # geocode addresses by rows and modifies 'Geo_Cols' if needed
    for i,p_r in P_df.iterrows():
        p_address_list = generate_address_list(p_r, p_r["Pat_Geo_Cols"], P_FALLBACK_SCHEMA)
        
        P_df.at[i, "Pat_Geocode"], p_al = geocode_address(geo_locator, p_address_list)#, retry_counter=1)
        if len(p_al) != len(p_address_list):
            P_df.at[i, "Pat_Geo_Cols"] = P_FALLBACK_SCHEMA[-len(p_al)] if len(p_al) else "DEFAULT_LOC"
            
    for i,c_r in C_df.iterrows():
        c_address_list = generate_address_list(c_r, c_r["Clinic_Geo_Cols"], C_FALLBACK_SCHEMA)
        
        C_df.at[i, "Clinic_Geocode"], c_al = geocode_address(geo_locator, c_address_list)#, retry_counter=1)
        if len(c_al) != len(c_address_list):
            C_df.at[i, "Clinic_Geo_Cols"] = C_FALLBACK_SCHEMA[-len(c_al)] if len(c_al) else "DEFAULT_LOC"
            
    # output processed dataframes
    try:
        if not os.path.isdir(output_path):
            os.makedirs(output_path)
        P_df.to_csv(os.path.join(output_path, 'P_df.csv'), index=False)
        C_df.to_csv(os.path.join(output_path, 'C_df.csv'), index=False)
    except:
        print("Fail to output processed Patients & Clinics dataframes, please check the output path is corret")
    
    return P_df, C_df