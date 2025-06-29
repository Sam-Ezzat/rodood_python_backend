import requests
import json
import aiohttp
from werkzeug.datastructures import headers
import config


###
### Create Label
################
async def add_custom_label(label_name, page_id):
    # Fixed: Corrected f-string format for URL
    access_token = config.get_access_token(page_id)
    url = f'https://graph.facebook.com/v20.0/me/custom_labels?access_token={access_token}'
    
    print(f"Creating label '{label_name}' for page {page_id}")
    print(f"URL: {url}")
    
    headers = {'Content-Type': 'application/json'}
    data = {
        'page_label_name': label_name
    }
    
    try:
        response = requests.post(url, headers=headers, json=data)
        print(f"Create label response: {response.text}")
        
        if response.status_code == 200:
            label_data = response.json()
            label_id = label_data.get('id')
            print(f"✅ Successfully created label '{label_name}' with ID: {label_id}")
            
            # Log label creation
            try:
                import sys
                from db_helper import get_db_connection, return_db_connection
                
                conn = get_db_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO activities 
                        (type, description, metadata, created_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (
                        'label-create', 
                        f'Created label {label_name}',
                        json.dumps({'page_id': page_id, 'label_id': label_id, 'label_name': label_name})
                    ))
                    conn.commit()
                    cursor.close()
                    return_db_connection(conn)
                    print(f"Label creation logged to database", file=sys.stderr)
            except Exception as db_error:
                print(f"Error logging label creation: {str(db_error)}")
                
            return label_id
        else:
            print(f"❌ Failed to create label. Status: {response.status_code}")
            print(f"Error: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Exception in add_custom_label: {str(e)}")
        return None


######
### Retrieve All labels For this Page
############################
def get_All_labels(page_id):
    # Fixed: Simplified API call - access token should only be in URL for GET requests
    access_token = config.get_access_token(page_id) 
    url = f'https://graph.facebook.com/v20.0/me/custom_labels?fields=id,page_label_name&access_token={access_token}'
    
    print(f"Getting all labels for page {page_id}")
    print(f"URL: {url}")
    
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.get(url, headers=headers)
        print(f"Get labels response status: {response.status_code}")
        
        if response.status_code == 200:
            label_data = response.json()
            labels_count = len(label_data.get('data', []))
            print(f"✅ Successfully retrieved {labels_count} labels for page {page_id}")
            
            # Log labels to database for tracking
            try:
                import sys
                from db_helper import get_db_connection, return_db_connection
                
                conn = get_db_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO activities 
                        (type, description, metadata, created_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (
                        'labels-list', 
                        f'Retrieved {labels_count} labels for page {page_id}',
                        json.dumps({'page_id': page_id, 'labels_count': labels_count})
                    ))
                    conn.commit()
                    cursor.close()
                    return_db_connection(conn)
                    print(f"Labels retrieval logged to database", file=sys.stderr)
            except Exception as db_error:
                print(f"Error logging labels retrieval: {str(db_error)}")
            
            return response.text
        else:
            print(f"❌ Failed to get labels for page {page_id}")
            print(f"Error: {response.text}")
            return json.dumps({"data": [], "error": response.text})
    except Exception as e:
        print(f"❌ Exception in get_All_labels: {str(e)}")
        return json.dumps({"data": [], "error": str(e)})


#######
# GET LABEL ID FOR A CUSTOM LABEL
###############################
async def get_label_id(page_label_name, page_id):
    All_labels = get_All_labels(page_id)
    All_labels = json.loads(All_labels)
    # print(All_labels)
    print(All_labels['data'])
    # labelId = ""
    for label in All_labels['data']:
        if page_label_name in label['page_label_name']:
            print("Found Label ID: ", label['id'])
            labelId = label['id']
            print(labelId)
            return labelId
        else:
            print("No label found Yet")

    return "No label found"


############################
async def Associate_Label_to_User(senderPSID, labelId, page_id):
    print("SenderPSID: ", senderPSID)
    print("LabelId: ", labelId)

    # Fixed: Correct data format according to Facebook Graph API documentation
    # The "user" param should be the only parameter in the body
    data = {
        "user": senderPSID
    }
    
    header = {'Content-Type': 'application/json'}
    access_token = config.get_access_token(page_id)
    url = f"https://graph.facebook.com/v20.0/{labelId}/label?access_token={access_token}"
    
    # Debug info
    print(f"Making label API call to: {url}")
    print(f"With data: {json.dumps(data)}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=header) as r:
                response_text = await r.text()
                print(f"Label API response: {response_text}")
                
                if r.status == 200:
                    print(f"✅ Custom label successfully added to user {senderPSID}")
                    
                    # Log this labeling action to database
                    try:
                        import sys
                        from db_helper import get_db_connection, return_db_connection
                        
                        conn = get_db_connection()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("""
                                INSERT INTO activities 
                                (type, description, metadata, created_at)
                                VALUES (%s, %s, %s, NOW())
                            """, (
                                'label-assign', 
                                f'Label {labelId} assigned to user {senderPSID}',
                                json.dumps({'page_id': page_id, 'sender_id': senderPSID, 'label_id': labelId})
                            ))
                            conn.commit()
                            cursor.close()
                            return_db_connection(conn)
                            print(f"Label assignment logged to database", file=sys.stderr)
                    except Exception as db_error:
                        print(f"Error logging label assignment: {str(db_error)}")
                    
                    return "RECEIVED EVENT", 200
                else:
                    print(f"❌ Failed to add label to user {senderPSID}. Status: {r.status}")
                    print(f"Error response: {response_text}")
                    return "RECEIVED EVENT", 200
    except Exception as e:
        print(f"❌ Exception in Associate_Label_to_User: {str(e)}")
        return "RECEIVED EVENT", 200


#############################
#check User Label
##########################
def Display_User_Label(senderPSID, page_id):
    # Fixed: Using f-string for better readability and consistency
    access_token = config.get_access_token(page_id)
    url = f"https://graph.facebook.com/v20.0/{senderPSID}/custom_labels?fields=id,page_label_name&access_token={access_token}"
    
    print(f"Checking labels for user {senderPSID} on page {page_id}")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url)
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            labels = response.json()
            print(f"✅ Found {len(labels.get('data', []))} labels for user {senderPSID}")
            
            # Log this check to database
            try:
                import sys
                from db_helper import get_db_connection, return_db_connection
                
                conn = get_db_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO activities 
                        (type, description, metadata, created_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (
                        'label-check', 
                        f'Checked labels for user {senderPSID}',
                        json.dumps({'page_id': page_id, 'sender_id': senderPSID, 'labels': labels.get('data', [])})
                    ))
                    conn.commit()
                    cursor.close()
                    return_db_connection(conn)
                    print(f"Label check logged to database", file=sys.stderr)
            except Exception as db_error:
                print(f"Error logging label check: {str(db_error)}")
                
            return labels
        else:
            print(f"❌ Error retrieving labels for user {senderPSID}")
            print(f"Error status: {response.status_code}")
            try:
                error_detail = response.json()
                print(f"Error details: {json.dumps(error_detail)}")
            except:
                print(f"Error response text: {response.text}")
            
            return {"data": [], "error": response.text}
    except Exception as e:
        print(f"❌ Exception in Display_User_Label: {str(e)}")
        return {"data": [], "error": str(e)}


######
# Update Label check if label id associated if yes good not delete All oldlabels      and add new label
#
########################
# def Update_label(new_label_Id, senderPSID):
#     labels = Display_User_Label(senderPSID)
#     for label in labels['data']:
#         if label['id'] == new_label_Id:
#             print("Label already exist")
#             return False
#         else:
#             Remove_label_from_User(senderPSID, labelId)
#             Associate_Label_to_User


###################
#
#
#####################
def Remove_label_from_User(senderPSID, labelId, page_id):
    # Fixed: Added missing ampersand (&) between URL parameters
    access_token = config.get_access_token(page_id)
    url = f"https://graph.facebook.com/v20.0/{labelId}/label?user={senderPSID}&access_token={access_token}"
    
    print(f"Making remove label API call to: {url}")
    
    header = {'Content-Type': 'application/json'}
    # No data needed for DELETE request with parameters in URL
    
    try:
        response = requests.delete(url=url, headers=header)
        print(f"Remove label response: {response.text}")
        
        if response.status_code == 200:
            print(f"✅ Successfully removed label {labelId} from user {senderPSID}")
            
            # Log this removal action
            try:
                import sys
                from db_helper import get_db_connection, return_db_connection
                
                conn = get_db_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO activities 
                        (type, description, metadata, created_at)
                        VALUES (%s, %s, %s, NOW())
                    """, (
                        'label-remove', 
                        f'Label {labelId} removed from user {senderPSID}',
                        json.dumps({'page_id': page_id, 'sender_id': senderPSID, 'label_id': labelId})
                    ))
                    conn.commit()
                    cursor.close()
                    return_db_connection(conn)
                    print(f"Label removal logged to database", file=sys.stderr)
            except Exception as db_error:
                print(f"Error logging label removal: {str(db_error)}")
                
            return True
        else:
            print(f"❌ Failed to remove label {labelId} from user {senderPSID}")
            return False
    except Exception as e:
        print(f"❌ Exception in Remove_label_from_User: {str(e)}")
        return False


#sender_Action_Typing_on_interaction for user
async def sender_Action_Typing_on(senderPsid, page_id):
    url = f"https://graph.facebook.com/v19.0/{page_id}/messages?access_token={config.get_access_token(page_id)}"
    header = {'Content-Type': 'application/json'}
    data = {"recipient": {"id": senderPsid}, "sender_action": "typing_on"}

    response = requests.post(url=url, headers=header, json=data)
    print("Typing on response: ", response.text)
    if response.status_code == 200:
        return "Typing on response Successfully: ", 200
    else:
        return "ERROR Typing on response: ", 400


#sender_Action_Typing_off_interaction for user
async def sender_Action_Typing_off(senderPsid, page_id):
    url = f"https://graph.facebook.com/v19.0/{page_id}/messages?access_token={config.get_access_token(page_id)}"
    header = {'Content-Type': 'application/json'}
    data = {
        "recipient": {
            "id": senderPsid,
        },
        "sender_action": "typing_off"
    }

    response = requests.post(url=url, headers=header, json=data)
    print(response.text)
    if response.status_code == 200:
        return "Typing off response successfully: ", 200
    else:
        return "ERROR Typing off response: ", 400


#leave note for follow up team to user to see up to date
def leave_note_on_user_profile(senderPsid, note_text, page_id):
    url = f'https://graph.facebook.com/v20.0/{senderPsid}/notes?access_token={config.get_access_token(page_id)}'
    header = {'Content-Type': 'application/json'}
    data = {'message': note_text}

    response = requests.post(url, headers=header, json=data)

    if response.status_code == 200:
        print('Note successfully added to user profile.')
    else:
        print(f'Failed to add note: {response.status_code} - {response.text}')
