# Author: Austin Shih
# Date: 22 Feb 2022

"""Cleans raw National Bridge Inventory Data and writes the output to a .csv file
Usage: src/clean.py --in_file=<in_file> --out_file=<out_file>
Options:
--in_file=<in_file>      Path to raw data folder
--out_file=<out_file>    Path to directory where the processed data should be written
"""

# python src/clean.py --in_file=data/raw --out_file=data/processed

# Imports 
from docopt import docopt
import pandas as pd
# import geopandas as gpd
import os

opt = docopt(__doc__)

def main(in_file, out_file):
    
    if os.path.exists(in_file) == False:
        print('Raw data directory does not exist, exiting script')
        exit()
    
    # Import raw data
    print('import raw data...')
    bridges = pd.read_csv('data/raw/nbi_raw.csv')
    # roads = gpd.read_file('data/raw/tl_2016_us_primaryroads/tl_2016_us_primaryroads.shp')

    # # clean roads geopandas df
    # print('clean geopandas file')
    # roads = roads.query('RTTYP == "I" | RTTYP == "U" | RTTYP == "S"')


    # select relevant columns
    rel_cols = [#'X',
                #'Y',
                #'FID',
                #'OBJECTID',
                'LATDD',      # latitude (converted) 16
                'LONGDD',     # longitude (converted) 17
                'STATE_CODE', # state code 1
                #'HIGHWAY_DI', # highway district 2
                'COUNTY_COD', # county code 3
                #'PLACE_CODE', # place code 4
                #'RECORD_TYP', # record type (route on or under structure) 5a
                'ROUTE_PREF', # route prefix (type of highway/route) 5b
                #'SERVICE_LE', # designated level of service 5c
                'ROUTE_NUMB', # route number 5d
                #'DIRECTION_', # direction suffix 5e
                'FEATURES_D', # features intersected 6
                #'FACILITY_C', # facilites carried by structure (similar to route) 7 
                #'LOCATION_0', # location 9
                #'MIN_VERT_C', # min vertical clearance 10
                #'KILOPOINT_', # kilometerpoint 11
                #'BASE_HWY_N', # base highway network? 12
                #'LRS_INV_RO', # LRS route number 13a
                #'SUBROUTE_N', # LRS subroute number 13b
                #'TOLL_020',   # toll status 20
                #'MAINTENANC', # maintenance responsibility 21
                'OWNER_022',  # owner 22
                #'FUNCTIONAL', # functional class 26
                'YEAR_BUILT', # year built 27
                #'DESIGN_LOA', # design load 31
                #'APPR_WIDTH', # approach road width 32
                #'MEDIAN_COD', # bridge median 33
                #'DEGREES_SK', # skew 34
                #'STRUCTUR_1', # structure flaired? 35
                #'NAVIGATION', # navigation control 38
                #'NAV_VERT_C', # navigation vertical clearance 39
                #'NAV_HORR_C', # navigation horizonatl clearance 40
                #'SERVICE_ON', # type of service 'on' bridge 42a
                #'SERVICE_UN', # tyoe if service 'under' bridge 42b
                'STRUCTUR_2', # structure kind 43a
                'STRUCTUR_3', # structure type 43b
                #'APPR_KIND_', # approach kind 44a
                #'APPR_TYPE_', # approach type 44b
                'MAIN_UNIT_', # number of spans in main unit 45
                #'APPR_SPANS', # number of approach spans 46
                #'HORR_CLR_M', # Total horizontal clearance (available max clearance) 47
                'MAX_SPAN_L', # length of max span 48
                'STRUCTUR_4', # structure length 49
                #'LEFT_CURB_', # left curb width 50a
                #'RIGHT_CURB', # right curb width 50b
                #'ROADWAY_WI', # roadway width (road + curbs) 51
                'DECK_WIDTH', # deck width (out-to-out) 52
                #'VERT_CLR_O', # min vertical clearance over 53
                #'VERT_CLR_U', # min vertical clearance under, ref. feature 54a
                #'VERT_CLR_1', # min vertical clearance under, from feature 54b
                #'LAT_UND_RE', # min lateral underclearance on right, ref. feature 55a
                #'LAT_UND_MT', # min lateral underclearance on right, from feature 55b
                #'LEFT_LAT_U', # min lateral underclearace on left, from feature 56
                'DECK_COND_', # deck condition 68
                'SUPERSTRUC', # superstructure condition 59
                'SUBSTRUCTU', # substructure condition 60
                #'CHANNEL_CO', # channel condition
                #'CULVERT_CO', # culvert condition
                'STRUCTURAL'] # structural evaluation 67
                #'DECK_GEOME', # deck geometry evaluation 68
                #'UNDCLRENCE', # underclearance evaluation 69
                #'WATERWAY_E', # waterway evaluation (adequacy) 71
                #'APPR_ROAD_', # approach roadway (alignment) evaluation (adequacy) 72
                #'DATE_OF_IN', # date of inspection (mmyy) 90
                #'INSPECT_FR', # inspection frequency 91
                #'BRIDGE_IMP', # bridge improvement cost 94
                #'ROADWAY_IM', # roadway improvement cost 95
                #'TOTAL_IMP_', # total improvement cost 96
                #'YEAR_OF_IM', # year of improvement cost estimate 97
                #'STRAHNET_H', # STRAHNET highway designation 100
                #'PARALLEL_S', # parallel structure designation 101
                #'TRAFFIC_DI', # direction of traffic 102
                #'HIGHWAY_SY', # on National Highway System (NHS)? 104
                #'FEDERAL_LA', # federal lands highway 105
                #'YEAR_RECON', # year reconstructed 106
                #'DECK_STRUC', # deck stucture type 107
                #'SURFACE_TY', # type of wear surface 108a
                #'MEMBRANE_T', # type of membrane 108b
                #'DECK_PROTE', # deck protection 108c
                #'NATIONAL_N', # designated national network for trucks? 110
                #'BRIDGE_LEN', # does bridge meet min NBIS length? 112
                #'SCOUR_CRIT', # scour critical bridges 113
                #'MIN_NAV_CL'] # min navigation vertical clearance 116

    bridges = bridges[rel_cols]

    # select only Interstate, US Numbered, and State highways
    bridges = bridges.query('ROUTE_PREF==1 | ROUTE_PREF==2 | ROUTE_PREF==3')

    # modifying values to make more sense 
    print('modifying values...')
    bridges = modify_clean_values(bridges)
    # roads = modify_geo(roads)


    # rename columns for readability
    print('renaming columns...')
    bridges = bridges.rename(columns = {
                                        'LATDD':      'latitude', 
                                        'LONGDD':     'longitude',
                                        'STATE_CODE': 'state_fips',
                                        'state_name': 'state_name',
                                        'state_abv': 'state_abv',
                                        'COUNTY_COD': 'county_code',
                                        'fips': 'fips',
                                        'ROUTE_PREF': 'route_type',
                                        'ROUTE_NUMB': 'route_num',
                                        'FEATURES_D': 'feature_intersect',
                                        'OWNER_022':  'owner',
                                        'YEAR_BUILT': 'year_built',
                                        #'SERVICE_ON': 'service_type',
                                        'STRUCTUR_2': 'bridge_material',
                                        'STRUCTUR_3': 'bridge_type',
                                        #'APPR_KIND_': 'appr_material',
                                        #'APPR_TYPE_': 'appr_type',
                                        'MAIN_UNIT_': 'num_span',
                                        #'APPR_SPANS': 'num_appr',
                                        'MAX_SPAN_L': 'max_span',
                                        'STRUCTUR_4': 'bridge_length',
                                        'DECK_WIDTH': 'bridge_width',
                                        'STRUCTURAL': 'eval_rating',
                                        'DECK_COND_': 'deck_condition',
                                        'SUPERSTRUC': 'superstructure_condition',
                                        'SUBSTRUCTU': 'substructure_condition'
                                        }
                            )
    
    bridges = bridges.drop(['state_fips', 'county_code'], axis=1)

    # save processed data
    print('saving clean data...')
    try:
        bridges.to_csv('data/processed/nbi_clean.csv', index=False)
        # roads.to_file('data/processed/us_roads.shp')
    except:
        os.makedirs(os.path.dirname(out_file))
        bridges.to_csv('data/processed/nbi_clean.csv', index=False)
        # roads.to_file('data/processed/us_roads.shp')

# # modify geo df
# def modify_geo(df):
    
#     df['RTTYP'] = df['RTTYP'].replace({
#         'I': 'Interstate highway',
#         'U': 'U.S. numbered highway',
#         'S': 'State highway'
#     })

#     return df

# modify value function
def modify_clean_values(df):
    # modifying values to make more sense 

    # drop bridges from territories
    indexState = df[(df['STATE_CODE'] == 60) | (df['STATE_CODE'] == 64) | (df['STATE_CODE'] == 66) | 
                    (df['STATE_CODE'] == 68) | (df['STATE_CODE'] == 69) | (df['STATE_CODE'] == 70) |
                    (df['STATE_CODE'] == 72) | (df['STATE_CODE'] == 74) | (df['STATE_CODE'] == 78)].index
    df = df.drop(indexState)

    # add two letter state code
    df['state_abv'] = df['STATE_CODE']
    df['state_abv'] = df['state_abv'].replace({
        1 : 'AL',
        2 : 'AK',
        4 : 'AZ',
        5 : 'AR',
        6 : 'CA',
        8 : 'CO',
        9 : 'CT',
        10: 'DE',
        11: 'DC',
        12: 'FL',
        13: 'GA',
        15: 'HI',
        16: 'ID',
        17: 'IL',
        18: 'IN',
        19: 'IA',
        20: 'KS',
        21: 'KY',
        22: 'LA',
        23: 'ME',
        24: 'MD',
        25: 'MA',
        26: 'MI',
        27: 'MN',
        28: 'MS',
        29: 'MO',
        30: 'MT',
        31: 'NE',
        32: 'NV',
        33: 'NH',
        34: 'NJ',
        35: 'NM',
        36: 'NY',
        37: 'NC',
        38: 'ND',
        39: 'OH',
        40: 'OK',
        41: 'OR',
        42: 'PA',
        44: 'RI',
        45: 'SC',
        46: 'SD',
        47: 'TN',
        48: 'TX',
        49: 'UT',
        50: 'VT',
        51: 'VA',
        53: 'WA',
        54: 'WV',
        55: 'WI',
        56: 'WY'
    })

    # update state names
    df['state_name'] = df['STATE_CODE'].replace({
        1: 'Alabama',
        2: 'Alaska',
        4: 'Arizona',
        5: 'Arkansas',
        6: 'California',
        8: 'Colorado',
        9: 'Connecticut',
        10: 'Delaware',
        11: 'District of Columbia',
        12: 'Florida',
        13: 'Georgia',
        15: 'Hawaii',
        16: 'Idaho',
        17: 'Illinois',
        18: 'Indiana',
        19: 'Iowa',
        20: 'Kansas',
        21: 'Kentucky',
        22: 'Louisiana',
        23: 'Maine',
        24: 'Maryland',
        25: 'Massachusett',
        26: 'Michigan',
        27: 'Minnesot',
        28: 'Mississippi',
        29: 'Missouri',
        30: 'Montana',
        31: 'Nebraska',
        32: 'Nevada',
        33: 'New Hampshire',
        34: 'New Jersey',
        35: 'New Mexico',
        36: 'New York',
        37: 'North Carolina',
        38: 'North Dakota',
        39: 'Ohio',
        40: 'Oklahoma',
        41: 'Oregon',
        42: 'Pennsylvania',     
        44: 'Rhode Island',
        45: 'South Carolina',
        46: 'South Dakota',
        47: 'Tennessee',
        48: 'Texas',
        49: 'Utah',
        50: 'Vermont',
        51: 'Virginia',
        53: 'Washington',
        54: 'West Virginia',
        55: 'Wisconsin',
        56: 'Wyoming'
    }) 

    # create full FIPS
    df['COUNTY_COD'] = df['COUNTY_COD'].map(str)
    df['COUNTY_COD'] = df['COUNTY_COD'].str.zfill(3)
    df['STATE_CODE'] = df['STATE_CODE'].map(str)
    df['STATE_CODE'] = df['STATE_CODE'].str.zfill(2)
    df['fips'] = df['STATE_CODE'] + df['COUNTY_COD']

    # create full route number
    df['ROUTE_NUMB'] = df['ROUTE_NUMB'].map(str)
    df['ROUTE_NUMB'] = df['ROUTE_NUMB'].str.lstrip('0')
    # define condition
    mask1 = (df['ROUTE_PREF'] == 1)
    mask2 = (df['ROUTE_PREF'] == 2)
    mask3 = (df['ROUTE_PREF'] == 3)
    # add appropriate hwy prefix 
    df.loc[mask1, 'ROUTE_NUMB'] = 'I- ' + df['ROUTE_NUMB'].astype(str)
    df.loc[mask2, 'ROUTE_NUMB'] = 'US Hwy ' + df['ROUTE_NUMB'].astype(str)
    df.loc[mask3, 'ROUTE_NUMB'] = 'State Rte/Hwy ' + df['ROUTE_NUMB'].astype(str)

    # update route type
    df['ROUTE_PREF'] = df['ROUTE_PREF'].replace({
        1: 'Interstate highway',
        2: 'U.S. numbered highway',
        3: 'State highway',     
        4: 'County highway',
        5: 'City street',
        6: 'Federal lands road',
        7: 'State lands road',
        8: 'Other'
    })

    # # add bridge deck area
    # df['deck_area'] = df.STRUCTUR_4 * df.DECK_WIDTH

    # # update route service level
    # df['SERVICE_LE'] = df['SERVICE_LE'].replace({
    #     0: 'None',
    #     1: 'Mainline',
    #     2: 'Alternate',
    #     3: 'Bypass',
    #     4: 'Spur',
    #     5: 'Business',
    #     6: 'Ramp, Wye, Connector, etc.',
    #     7: 'Service and/or unclassified frontage road'
    # })

    # update owner
    df['OWNER_022'] = df['OWNER_022'].replace({
        1: 'State Highway Agency',
        2: 'County Highway Agency',
        3: 'Town or Township Highway Agency',
        4: 'City or Municipal Highway Agency',
        11: 'State Park, Forest, or Reservation Agency',
        12: 'Local Park, Forest, or Reservation Agency',
        21: 'Other State Agencies',
        25: 'Other Local Agencies',
        26: 'Private (other than railroad)',
        27: 'Railroad',
        31: 'State Toll Authority',
        32: 'Local Toll Authority',
        60: 'Other Federal Agencies (not listed below)',
        61: 'Indian Tribal Government',
        62: 'Bureau of Indian Affairs',
        63: 'Bureau of Fish and Wildlife',
        64: 'U.S. Forest Service',
        66: 'National Park Service',
        67: 'Tennessee Valley Authority',
        68: 'Bureau of Land Management',
        69: 'Bureau of Reclamation',
        70: 'Corps of Engineers (Civil)',
        71: 'Corps of Engineers (Military)',
        72: 'Air Force',
        73: 'Navy/Marines',
        74: 'Army',
        75: 'NASA',
        76: 'Metropolitan'
    })

    # # update type of service 'on'
    # df['SERVICE_ON'] = df['SERVICE_ON'].replace({
    #     1: 'Highway',
    #     2: 'Railroad',
    #     3: 'Pedestrian-bicycle',
    #     4: 'Highway-railroad',
    #     5: 'Highway-pedestrian',
    #     6: 'Overpass structure at an interchange',
    #     7: 'Third level (Interchange)',
    #     8: 'Fourth level (Interchange)',
    #     9: 'Building or plaza',
    #     0: 'Other'
    # })

    # update structure material
    df['STRUCTUR_2'] = df['STRUCTUR_2'].replace({
        1: 'Concrete',
        2: 'Concrete continuous',
        3: 'Steel',
        4: 'Steel continuous',
        5: 'Prestressed concrete (post-tension)',
        6: 'Prestressed concrete continuous (post-tension)',
        7: 'Wood or Timber',
        8: 'Masonry',
        9: 'Aluminum, Wrought Iron, or Cast Iron',
        0: 'Other'
    })

    # update structure type
    df['STRUCTUR_3'] = df['STRUCTUR_3'].replace({
        1: 'Slab',
        2: 'Stringer/Multi-beam or Girder',
        3: 'Girder and Floorbeam System',
        4: 'Tee Beam',
        5: 'Box Beam or Girders - Multiple',
        6: 'Box Beam or Girders - Single or Spread',
        7: 'Frame (except frame culverts)',
        8: 'Orthotropic',
        9: 'Truss - Deck',
        10: 'Truss - Thru',
        11: 'Arch - Deck',
        12: 'Arch - Thru',
        13: 'Suspension',
        14: 'Stayed Girder',
        15: 'Movable - Lift',
        16: 'Movable - Bascule',
        17: 'Movable - Swing',
        18: 'Tunnel',
        19: 'Culvert (includes frame culverts)',
        20: 'Mixed types',
        21: 'Segmental Box Girder',
        22: 'Channel Beam',
        0: 'Other'
    })

    # # update approach material
    # df['APPR_KIND_'] = df['APPR_KIND_'].replace({
    #     1: 'Concrete',
    #     2: 'Concrete continuous',
    #     3: 'Steel',
    #     4: 'Steel continuous',
    #     5: 'Prestressed concrete (post-tension)',
    #     6: 'Prestressed concrete continuous (post-tension)',
    #     7: 'Wood or Timber',
    #     8: 'Masonry',
    #     9: 'Aluminum, Wrought Iron, or Cast Iron',
    #     0: 'Other'
    # })

    # # update approach type
    # df['APPR_TYPE_'] = df['APPR_TYPE_'].replace({
    #     1: 'Slab',
    #     2: 'Stringer/Multi-beam or Girder',
    #     3: 'Girder and Floorbeam System',
    #     4: 'Tee Beam',
    #     5: 'Box Beam or Girders - Multiple',
    #     6: 'Box Beam or Girders - Single or Spread',
    #     7: 'Frame (except frame culverts)',
    #     8: 'Orthotropic',
    #     9: 'Truss - Deck',
    #     10: 'Truss - Thru',
    #     11: 'Arch - Deck',
    #     12: 'Arch - Thru',
    #     13: 'Suspension',
    #     14: 'Stayed Girder',
    #     15: 'Movable - Lift',
    #     16: 'Movable - Bascule',
    #     17: 'Movable - Swing',
    #     18: 'Tunnel',
    #     19: 'Culvert (includes frame culverts)',
    #     20: 'Mixed types',
    #     21: 'Segmental Box Girder',
    #     22: 'Channel Beam',
    #     0: 'Other'
    # })

    # add verbose structural rating 
    df['STRUCTURAL'] = df['STRUCTURAL'].replace({'*': '-1'}) # change 'none' to number
    df['STRUCTURAL'] = pd.to_numeric(df['STRUCTURAL']) # make ratings numeric
    df['eval_rating_v'] = df['STRUCTURAL']

    df['eval_rating_v'] = df['eval_rating_v'].replace({
        -1: 'None',
        0: 'Failed',
        1: 'Imminent Failure',
        2: 'Critical',
        3: 'Serious',
        4: 'Poor',
        5: 'Fair',
        6: 'Satisfactory',
        7: 'Good',
        8: 'Very Good',
        9: 'Excellent'
    })

    # change condition rating to verbose
    df['DECK_COND_'] = df['DECK_COND_'].replace({'N': '-1'}) # change 'none' to number
    df['DECK_COND_'] = pd.to_numeric(df['DECK_COND_']) # make ratings numeric
    # df['DECK_COND_'] = df['DECK_COND_'].replace({
    #     -1: 'None',
    #     0: 'Failed',
    #     1: 'Imminent Failure',
    #     2: 'Critical',
    #     3: 'Serious',
    #     4: 'Poor',
    #     5: 'Fair',
    #     6: 'Satisfactory',
    #     7: 'Good',
    #     8: 'Very Good',
    #     9: 'Excellent'
    # })
    df['SUPERSTRUC'] = df['SUPERSTRUC'].replace({'N': '-1'}) # change 'none' to number
    df['SUPERSTRUC'] = pd.to_numeric(df['SUPERSTRUC']) # make ratings numeric
    # df['SUPERSTRUC'] = df['SUPERSTRUC'].replace({
    #     -1: 'None',
    #     0: 'Failed',
    #     1: 'Imminent Failure',
    #     2: 'Critical',
    #     3: 'Serious',
    #     4: 'Poor',
    #     5: 'Fair',
    #     6: 'Satisfactory',
    #     7: 'Good',
    #     8: 'Very Good',
    #     9: 'Excellent'
    # })
    df['SUBSTRUCTU'] = df['SUBSTRUCTU'].replace({'N': '-1'}) # change 'none' to number
    df['SUBSTRUCTU'] = pd.to_numeric(df['SUBSTRUCTU']) # make ratings numeric
    # df['SUBSTRUCTU'] = df['SUBSTRUCTU'].replace({
    #     -1: 'None',
    #     0: 'Failed',
    #     1: 'Imminent Failure',
    #     2: 'Critical',
    #     3: 'Serious',
    #     4: 'Poor',
    #     5: 'Fair',
    #     6: 'Satisfactory',
    #     7: 'Good',
    #     8: 'Very Good',
    #     9: 'Excellent'
    # })

    return df

if __name__ == "__main__":
  main(opt["--in_file"], opt["--out_file"])