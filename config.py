API_KEY = ""

COMPONENT_RESTRICTIONS = { 'country': 'CA'}

P_GEO_COLS = ["Address", "Postal Code"]
C_GEO_COLS = ["Clinic Address", "Postal Code"]

N = 6
RETRY_COUNTER_CONST = 10

P_FALLBACK_SCHEMA = ['FSA', 'City', 'Province']
C_FALLBACK_SCHEMA = ['FSA', 'Clinic City', 'Province']

OUTPUT_COLS = ["Patient_ID","Pat_Geo_Cols","Pat_Geocode","Pat_Address","Pat_Postal_Code",
    "Pat_FSA","Nearest_Clinic_ID","Clinic_Geo_Cols","Clinic_Geocode",
    "Clinic_Address","Clinic_Postal_Code","Clinic_FSA","Clinic_Distance"]
    
DEFAULT_LOC = [43.6532, 79.3832]
DEFAULT_DIST = 9999
