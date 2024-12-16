import pymongo 

from pymongo.operations import UpdateOne

import pandas as pd

import os

import traceback

import hashlib

import re

from datetime import datetime

from unidecode import unidecode

from urllib.parse import quote_plus

drive_to_list = '/home/STFS0030M/CDMS_Multiple_Suki_Card/CDMS_Multiple_Suki_Card_2024'


mongo_username = "PD3079P"
mongo_password = "EibKMRGjb8@$Yj3h"
mongo_db_name = "mydb"
mongo_collection_name = "PTCCUSTOMERDETAILS76"
encoded_username = quote_plus(mongo_username)
encoded_password = quote_plus(mongo_password)
mongo_uri = f"mongodb://{encoded_username}:{encoded_password}@10.214.2.22:37017/mydb"

client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db_name]
collection = db[mongo_collection_name]

dff=pd.DataFrame()
CSVfile_name = 'processed_Suki_Card_file.csv'
#if os.path.exists(CSVfile_name):
#    dff = pd.read_csv(CSVfile_name
#else:
#    dff = pd.DataFrame(columns=['source'])


def hash_df(df, columns, hash_value):
    # Concatenate specified columns
    concat_str = df[columns].astype(str).agg(''.join, axis=1)
    
    # Compute hash for each row
    df[hash_value] = concat_str.apply(lambda x: hashlib.sha512(x.encode('utf-8')).hexdigest())
    
    return df

def change_accent(row):

    row['FIRSTNAME']= unidecode(row['FIRSTNAME'])

    row['LASTNAME']= unidecode(row['LASTNAME'])

    row['FIRSTNAME'] = row['FIRSTNAME'].upper()   
 
    row['LASTNAME'] = row['LASTNAME'].upper()

    return row

def special_character_check_firstname(row):

    first_name = row['FIRSTNAME']
    
    last_name = row['LASTNAME']

    while(True):

        special_char = re.compile(r'[^a-zA-Z0-9.]')
    
        if special_char.search(row['FIRSTNAME'].replace(' ','').replace('-','')) != None:
            
            if len(row['LASTNAME'].split(' '))>1:
                
                temp_last_name = row['LASTNAME'].split(' ')
                
                row['FIRSTNAME'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
                
                row['LASTNAME'] = temp_last_name[len(temp_last_name)-1]
            
            else:
                
                row['valid'] = 'invalid'
    
                if row['reason']=='':
                
                    row['reason'] = 'special characters in First name'
                    
                else:
                    
                    row['reason'] += ',Special characters in first name'


        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):
            
            break
        
        else:

            first_name = row['FIRSTNAME']
            
            last_name = row['LASTNAME']                
            
            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)

        row = number_check_firstname(row)

        row = number_check_lastname(row)
                
            
            
    return row

def special_character_check_lastname(row):
    
    first_name = row['FIRSTNAME']
    
    last_name = row['LASTNAME']

    while(True):


        special_char = re.compile(r'[^a-zA-Z0-9.]')
        
        if special_char.search(row['LASTNAME'].replace(' ','').replace('-','')) != None:
            
            if len(row['FIRSTNAME'].split(' '))>1:
                
                temp_last_name = row['FIRSTNAME'].split(' ')
                
                row['FIRSTNAME'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
                
                row['LASTNAME'] = temp_last_name[len(temp_last_name)-1]
            
            else:
                
                row['valid'] = 'invalid'
                
                
                if row['reason']=='':
                
                    row['reason'] = 'Special characters in last name'
                    
                else:
                    
                    row['reason'] += ',Special characters in last name'
                    

        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):
            
            break
        
        else:

            first_name = row['FIRSTNAME']
            
            last_name = row['LASTNAME']                
            
            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)

        row = number_check_firstname(row)

        row = number_check_lastname(row)
                
        row = special_character_check_firstname(row)

    return row

def number_check_firstname(row):
    
    first_name = row['FIRSTNAME']
    
    last_name = row['LASTNAME']

    while(True):

    
        if len(re.findall(r'[0-9]+', row['FIRSTNAME']))>0:
            
            
            if len(row['LASTNAME'].split(' '))>1:
                
                
                temp_last_name = row['LASTNAME'].split(' ')
                
                row['FIRSTNAME'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
                
                row['LASTNAME'] = temp_last_name[len(temp_last_name)-1]
            
            else:
                
                
                row['valid'] = 'invalid'
    
                if row['reason']=='':
                
                    row['reason'] = 'Numeric characters in First name'
                    
                else:
                    
                    row['reason'] += ',Numeric characters in first name'
            

        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):
            
            break
        
        else:

            first_name = row['FIRSTNAME']
            
            last_name = row['LASTNAME']                
            
            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)
            
    return row

def number_check_lastname(row):

    first_name = row['FIRSTNAME']
    
    last_name = row['LASTNAME']

    while(True):
    
        if len(re.findall(r'[0-9]+', row['LASTNAME']))>0:
            
            if len(row['FIRSTNAME'].split(' '))>1:
                
                temp_last_name = row['FIRSTNAME'].split(' ')
                
                row['FIRSTNAME'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
                
                row['LASTNAME'] = temp_last_name[len(temp_last_name)-1]
            
            else:
                
                row['valid'] = 'invalid'
                
                
                if row['reason']=='':
                
                    row['reason'] = 'Numeric characters in last name'
                    
                else:
                    
                    row['reason'] += ',Numeric characters in last name'


        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):
            
            break
        
        else:
            
            first_name = row['FIRSTNAME']
            
            last_name = row['LASTNAME']                
            
            pass


        row = single_character_check_firstname(row)

        row = single_character_check_lastname(row)

        row = number_check_firstname(row)

    return row

def single_character_check_firstname(row):
    
    if len(row['FIRSTNAME'])<2:
        
        if len(row['LASTNAME'].split(' '))>1:
            
            temp_last_name = row['LASTNAME'].split(' ')
            
            row['FIRSTNAME'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row['LASTNAME'] = temp_last_name[len(temp_last_name)-1]
            

        
        else:
            
            row['valid'] = 'invalid'
            
            if row['FIRSTNAME']=='':
                    
                row['reason'] = 'Null Value in first name'

            else:
                
                row['reason'] = 'Single character in first name'
            
    return row
   
def single_character_check_lastname(row):
    

    first_name = row['FIRSTNAME']
    
    last_name = row['LASTNAME']

    while(True):
    
        if len(row['LASTNAME'])<2:
            
            if len(row['FIRSTNAME'].split(' '))>1:
                
                temp_last_name = row['FIRSTNAME'].split(' ')
                
                row['FIRSTNAME'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
                
                row['LASTNAME'] = temp_last_name[len(temp_last_name)-1]
                
            
            else:
                
                row['valid'] = 'invalid'
                
                if row['LASTNAME']=='':
                    
                
                    if row['reason']=='':
                    
                        row['reason'] = 'Null value in last name'
                        
                    else:
                        
                        row['reason'] += ',Null value in last name'
                
                else:
                    
                    if row['reason']=='':
                    
                        row['reason'] = 'Single character in last name'
                        
                    else:
                        
                        row['reason'] += ',Single character in last name'
    

        
            row = single_character_check_firstname(row)
        
        if ((row['FIRSTNAME']==first_name) & (row['LASTNAME']==last_name)):
            
            break
        
        else:
            
            first_name = row['FIRSTNAME']
            
            last_name = row['LASTNAME']                

            pass


    return row
   

def list_all_files_in_drive(drive):

    all_files = []

    for root, dirs, files in os.walk(drive):

        for file in files:

            file_path = os.path.join(root, file)

            all_files.append(file_path)

    return all_files

input="CDMS_Multiple_Suki_Card"

output="CDMS_Multiple_Suki_Card_unmatched"

#drive_to_list = '/home/STFS0030M/CDMS_Multiple_Suki_Card/CDMS_Multiple_Suki_Card_sample'

files_in_drive = list_all_files_in_drive(drive_to_list)

# files_location = ["/home/pythonFiles/CDMS_Multiple_Suki_Card.csv"]

files_location=[]

for file in files_in_drive:
    
    file = file.replace('\\','/')
    
    list1 = file.split('/')    
        
    list1 = '/'.join(list1)
                       
    files_location.append(list1)

print(files_location)

for file_path in files_location:
    
    try:

        i, filename = os.path.split(file_path)

        if os.path.exists(CSVfile_name):
            dff = pd.read_csv(CSVfile_name)
        else:
            dff = pd.DataFrame(columns=['source'])

        if filename in dff['source'].values:

            print(filename + " already Processed")
            continue

        if os.path.exists(i.replace(input,output)):
            pass
        else:
            os.makedirs(i.replace(input,output))

        if (filename) in os.listdir(i) and filename.endswith('.csv'):

            cunk_size = 5000

            if os.path.exists(i.replace(input,output)+"//invalid_"+filename):

                os.remove(i.replace(input,output)+"//invalid_"+filename)

            if os.path.exists(i.replace(input,output)+"//Unmatched_"+filename):

                os.remove(i.replace(input,output)+"//Unmatched_"+filename)

            for j, chunk_df in enumerate(pd.read_csv(i+"//"+filename, chunksize=cunk_size, low_memory=False,encoding='utf-16')):

                print(len(chunk_df))

                # continue

                # start_index = j * cunk_size

                # end_index = min((j + 1) * cunk_size, total_rows)

                # # print(f"Processing chunk {j+1}: Rows {start_index}-{end_index}")

                # CDMS_merged = df_[start_index:end_index]

                df = chunk_df

                df.fillna('', inplace=True)

                suffixes = [' JR.', ' SR.', ' JR', ' SR', ' I', ' II', ' III', ' IV', ' V']    

                # Apply the pattern to remove the suffixes
                for suffix in suffixes:
                    pattern = re.escape(suffix) + r'\b'  # Adding word boundary to match suffixes
                    df['FIRSTNAME'] = df['FIRSTNAME'].apply(lambda x: re.sub(pattern, '', str(x)).strip())
                    df['LASTNAME'] = df['LASTNAME'].apply(lambda x: re.sub(pattern, '', str(x)).strip())

                df['valid'] = 'valid'
                    
                df['reason'] = ''

                df = df.apply(lambda row:change_accent(row),axis = 1)
                    
                # single character check                
                
                df = df.apply(lambda row:single_character_check_firstname(row),axis = 1)
                
                df = df.apply(lambda row:single_character_check_lastname(row),axis = 1)

                # print('single character check    ',datetime.now()) 
                
                # numeric character check
                
                df = df.apply(lambda row:number_check_firstname(row),axis = 1)
                
                df = df.apply(lambda row:number_check_lastname(row),axis = 1)

                # print('numeric character check   ',datetime.now()) 

                # special character check
                    
                df = df.apply(lambda row:special_character_check_firstname(row),axis = 1)
                
                df = df.apply(lambda row:special_character_check_lastname(row),axis = 1)

                # print('special character check   ',datetime.now()) 

                invalid_df = df[df['valid']=='invalid']
                                                
                valid_df = df[df['valid']=='valid']

                invalid_df.to_csv(i.replace(input,output)+"//invalid_"+filename,index  = False, mode='a', header=not os.path.exists(i.replace(input,output)+"//invalid_"+filename)) 

                valid_df = hash_df(valid_df, ['FIRSTNAME', 'LASTNAME', 'BIRTHDATE'], 'HASH_1')

                unique_df = valid_df[~(valid_df.duplicated(['HASH_1']))]

                # unique_df=unique_df[0:2]

                values_to_search = unique_df['HASH_1'].unique().tolist()

                # Query MongoDB for the values that are present in the database
                query = {"2392": {"$in": values_to_search}}
                
                # Find matching documents and convert to list
                results = list(collection.find(query))
                present_values = {doc["2392"] for doc in results}  # Set of present values

                # Step 3: Identify values in `values_to_search` that are not in `present_values`
                missing_values = set(values_to_search) - present_values

                missing_records_df = valid_df[valid_df['HASH_1'].isin(missing_values)]

                missing_records_df.to_csv(i.replace(input,output)+"//Unmatched_"+filename,index  = False, mode='a', header=not os.path.exists(i.replace(input,output)+"//Unmatched_"+filename)) 

                # Print the original results (for debugging purposes)
                print("Original results:")

                bulk_operations = []

                for result in results:

                    # print(result)

                    mongo_2004_value = result['2392']

                    # Search for the '2004' value in the DataFrame
                    search_result = valid_df[valid_df['HASH_1'] == mongo_2004_value]

                    # df_selected = search_result[['CARDNO', 'AVAILABLEPOINTS']]

                    search_result['AVAILABLEPOINTS'] = pd.to_numeric(search_result['AVAILABLEPOINTS'], errors='coerce')

                    df_sorted = search_result.sort_values(by='AVAILABLEPOINTS', ascending=False)

                    # Drop duplicates based on CARDNO, keeping the first occurrence (which will have the highest AVAILABLEPOINTS due to sorting)
                    df_deduplicated = df_sorted.drop_duplicates(subset='CARDNO', keep='first')

                    # Convert the selected columns to a list of dictionaries
                    combined_list = df_deduplicated.to_dict('records')

                    existing_cards = result.get('2253', [])

                    #print(existing_cards)
                    #print(type(existing_cards))
                    max_id = 0
                    card_dict = {}
                    if not isinstance(existing_cards, str):
                        if existing_cards:
                            max_id = max([card['id'] for card in existing_cards])
                            # Convert existing cards list into a dictionary for quick lookup by CARDNO
                            card_dict = {card['CARDNO']: card for card in existing_cards}

                    # print(card_dict)
                    # print(combined_list)

                    for new_card in combined_list:
                        cardno = new_card['CARDNO']
                        if cardno in card_dict:
                            # If CARDNO exists, update the AVAILABLEPOINTS
                            card_dict[cardno]['AVAILABLEPOINTS'] = new_card['AVAILABLEPOINTS']
                            card_dict[cardno]['CARDTYPE'] = new_card['CARDTYPE']
                            card_dict[cardno]['ISSUINGBRANCH'] = new_card['ISSUINGBRANCH']
                            card_dict[cardno]['ISSUEDON'] = new_card['ISSUEDON']
                            card_dict[cardno]['EXPIRYDATE'] = new_card['EXPIRYDATE']
                        else:
                            # If CARDNO does not exist, assign a new id and add the new card
                            max_id += 1
                            card_dict[cardno] = {
                                                    'id': max_id,
                                                    'CARDNO': new_card['CARDNO'],
                                                    'AVAILABLEPOINTS': new_card['AVAILABLEPOINTS'],
                                                    'CARDTYPE': new_card['CARDTYPE'],
                                                    'ISSUINGBRANCH': new_card['ISSUINGBRANCH'],
                                                    'ISSUEDON': new_card['ISSUEDON'],
                                                    'EXPIRYDATE': new_card['EXPIRYDATE']
                                                }
                    
                    # Convert the dictionary back to a list
                    updated_cards = list(card_dict.values())
                    # print(updated_cards)
                    bulk_operations.append(
                        UpdateOne(
                            {'_id': result['_id']},  # Match the document by its _id
                            {'$set': {'2253': updated_cards}}  # Set the updated '2003' array
                        )
                    )
                
                if bulk_operations:
                    result = collection.bulk_write(bulk_operations)
                    print(f"Matched {result.matched_count} documents, modified {result.modified_count} documents.")
                    # print("Test Operation")
                else:
                    print("No operations to update.")

        new_row = pd.DataFrame({'source': [filename]})
        dff = pd.concat([dff, new_row], ignore_index=True)
        dff.to_csv(CSVfile_name, index=False)

    except Exception as e:
    
        print(str(e))            
        
        print(traceback.print_exc())
