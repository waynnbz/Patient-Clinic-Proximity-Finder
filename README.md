
# Final design

- Let's do two approaches
    1. Pure Google
        - Using Google Geocoding API to obtain lat/lng
        - Using geopy.distance.geodesic to filter the clinic into N nearest ones in terms of geodesic distance (N=5/6 suggested after explore the clinic distribution on map
        - Finally using Google distance matrix API to get the shortest travel distance for the N clinics and output the nearest one
    2. Possibly free: 
        - [Geocode.ca](http://geocode.ca) throttled port for lat/lng
            - (tested to be more accurate than Google but throttle control applies in free port)
        - Same N pool processs using geodesic distance
        - Use OSMnx to gain a node/edge a graph centered at Patient node with a radius by considering N pool dist range
        - Finally using Networkx to compute the shortest path locally from the OSMnx graph

- Geocode detection failed case
    - Shouldn't break the code and still should be able to recommend clinics
        - 'Postal Code' <  'FSA' <  'City' < 'Province'

- Retry on failure:

    ```jsx
    # To retry because intermittent failures sometimes occurs
    except (GeocoderQueryError) as error:
        if retry_counter < RETRY_COUNTER_CONST:
            return geocode_address(geo_locator, line_address, component_restrictions, retry_counter + 1)
        else:
            location_result = {"Lat": 0, "Long": 0, "Error": error.message, "formatted_address": "",
                               "location_type": ""}
    ```

-   During geocoding process, the will run through the fallback schema list recursively if best address failed, and in worst case scenario fall back to default values set in config file

- I've switched to the Google geocoder as Nominatim is pretty sensitive to missing information, Google hammers through it no problem

- 'Postal Code' fail rate: 3/100 for C_df
    - however, all 3 failed cases are later correctly geocoded after adding a component filter country:CA
        - One building offset, comparing the result from component added failed case to its physical address
    - **TYPO**, there's typo case (eg. V3Z 6S7 which mapped to V3S 6S7)
        - By adding physical 'address' GoogleV3 still able to figure out the correct address
        - Strangely enough, V3Z 6S7 is able to find a locale from OSM Geocoder.CA (and all other failed GoogleV3 case), same locale as from Google but labeled as input V3Z 6S7. Maybe the typo is from Google???
        - I guess this proves that [Geocoder.CA](http://geocoder.CA) is more accurate than GoogleV3 (w/ country component set to Canada)
- 'Address' alone fail rate 11/100 (expected)
    - However, with country component filtering, the failed cases only reduced 1, yields 10/100
- Cost and rate limit:
    - for 0-100,000: $0.005 per each
    - 50 request per second(QPS)