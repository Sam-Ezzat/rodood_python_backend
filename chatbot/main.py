import random
from flask import jsonify
import threading
import json
import asyncio
import re
from types import new_class
import aiohttp
from flask.wrappers import Response
from openai.types.beta import assistant
from quart import Quart, request
import config
from datetime import datetime, timedelta
import time
import handle_message
import sentiment
import handeling_User
import requests
from flask import render_template
from flask import Flask, request
import labeling
import assistant_handler

################################################
#  OPENAI ASSISTANT CHANGING END
################################################

#####################
#
############################
app = Flask(__name__)
app.config[
    'SECRET_KEY'] = 'efcd6c5e2b8b4d9f5ad7d369eb9374c4' or config.INSTAGRAM_APP_SECRET

#Function to access the Sender API


@app.route('/home', methods=["GET", "POST"])
def home():

  return 'HOME'


@app.route('/data-deletion', methods=['POST'])
async def data_deletion_callback():
  signed_request = request.form.get('signed_request')
  if not signed_request:
    return jsonify({'error': 'No signed_request parameter'}), 400

  try:
    # Split the signed request
    encoded_sig, payload = signed_request.split('.')

    # Decode the payload
    from base64 import urlsafe_b64decode

    def base64_url_decode(inp):
      padding = '=' * (4 - (len(inp) % 4))
      return urlsafe_b64decode(inp + padding)

    data = json.loads(base64_url_decode(payload))
    user_id = data.get('user_id')

    if not user_id:
      return jsonify({'error': 'Invalid payload'}), 400

    # Generate unique confirmation code
    confirmation_code = ''.join(random.choices('0123456789ABCDEF', k=8))

    # Start data deletion process
    # Remove user data from user_state if it exists
    if user_id in user_state:
      # Save deletion status for verification
      deletion_status = {
          'user_id': user_id,
          'deletion_time': datetime.now().isoformat(),
          'status': 'processing'
      }

      # Delete user conversation history and state
      try:
        # Clear all user data from user_state
        del user_state[user_id]
        deletion_status['status'] = 'completed'
      except Exception as e:
        deletion_status['status'] = f'failed: {str(e)}'

      # Store deletion status for verification
      if not hasattr(app, 'deletion_records'):
        app.deletion_records = {}
      app.deletion_records[confirmation_code] = deletion_status

    response_data = {
        'url':
        f'https://{request.host}/deletion-status?id={confirmation_code}',
        'confirmation_code': confirmation_code
    }

    return jsonify(response_data)

  except Exception as e:
    return jsonify({'error': str(e)}), 500


@app.route('/deletion-status')
def deletion_status():
  deletion_id = request.args.get('id')
  if not deletion_id:
    return "Invalid deletion request", 400

  if not hasattr(app, 'deletion_records'):
    return "No deletion records found", 404

  deletion_record = app.deletion_records.get(deletion_id)
  if not deletion_record:
    return "Deletion request not found", 404

  return jsonify({
      'status': deletion_record['status'],
      'deletion_time': deletion_record['deletion_time'],
      'user_id': deletion_record['user_id']
  })


@app.route('/', methods=["GET", "POST"])
async def index():
  if request.method == 'GET':
    #do something.....
    VERIFY_TOKEN = config.VERIFY_TOKEN
    # labeling.get_All_labels()
    # labeling.get_label_id()
    if 'hub.mode' in request.args:
      mode = request.args.get('hub.mode')
      print(mode)
    if 'hub.verify_token' in request.args:
      token = request.args.get('hub.verify_token')
      print(token)
    if 'hub.challenge' in request.args:
      challenge = request.args.get('hub.challenge')
      print(challenge)

    if 'hub.mode' in request.args and 'hub.verify_token' in request.args:
      mode = request.args.get('hub.mode')
      token = request.args.get('hub.verify_token')

      if mode == 'subscribe' and token == VERIFY_TOKEN:
        print('WEBHOOK VERIFIED')

        challenge = request.args.get('hub.challenge')

        return challenge, 200
      else:
        return 'ERROR', 403

    return 'SOMETHING', 200

  if request.method == 'POST':
    #do something.....
    VERIFY_TOKEN = config.VERIFY_TOKEN
    # labeling.add_custom_label("Positive Chat")
    # labeling.add_custom_label("Negative Chat")
    if 'hub.mode' in request.args:
      mode = request.args.get('hub.mode')
      print(mode)
    if 'hub.verify_token' in request.args:
      token = request.args.get('hub.verify_token')
      print(token)
    if 'hub.challenge' in request.args:
      challenge = request.args.get('hub.challenge')
      print(challenge)

    if 'hub.mode' in request.args and 'hub.verify_token' in request.args:
      mode = request.args.get('hub.mode')
      token = request.args.get('hub.verify_token')

      if mode == 'subscribe' and token == VERIFY_TOKEN:
        print('WEBHOOK VERIFIED')

        challenge = request.args.get('hub.challenge')

        return challenge, 200
      else:
        return 'ERROR', 403

    #do something else
    data = request.data
    body = json.loads(data.decode('utf-8'))
    # print("body: ",body)

    # Make sure 'object' is in body and handle both page and instagram objects
    if 'object' in body and body['object'] in ['page', 'instagram']:
      entries = body['entry']
      tasks = []
      print(f"Webhook event received with object '{body['object']}' containing {len(entries)} entries")
      for entry in entries:
        if "messaging" in entry:
          print("Entry with messaging: ", entry)
          
          # Instagram-specific logging
          is_instagram = body['object'] == 'instagram'
          if is_instagram:
            print("INSTAGRAM MESSAGE DETECTED!")
            print("Instagram message entry:", entry)
          
          webhookEvent = entry["messaging"][0]
          page_id = entry["id"]
          
          # For Instagram, we need to convert the Instagram page ID to the linked Facebook page ID
          if is_instagram:
            print(f"Instagram webhook received with original page_id: {page_id}")
            # Map Instagram ID to Facebook page ID (they share the same access token)
            original_id = page_id
            
            # CRITICAL MAPPING: We must use the Facebook page ID for all Instagram messages
            # This ensures the correct access token and assistant ID are used
            page_id = config.get_page_id_from_instagram_id(page_id)
            print(f"✓ Instagram ID {original_id} mapped to Facebook page ID {page_id}")
          else:
            # Keep Facebook page ID as is
            print(f"Facebook message with page_id: {page_id}")
            
          config.PAGE_ID = page_id
          print(webhookEvent)
          senderPsid = webhookEvent['sender']['id']
          print('Sender PSID: {}'.format(senderPsid))
          if 'message' in webhookEvent:
            await handle_message.merge_user_messages(senderPsid,
                                                     webhookEvent['message'],
                                                     page_id)
            asyncio.create_task(
                assistant_handler.check_sentiment_every_24_hours())
            # Return success immediately to Facebook
            # return 'EVENT_RECEIVED', 200
          if tasks:
            print("get into tasks gather")
            await asyncio.gather(*tasks)
            return 'EVENT_RECEIVED', 200
          else:
            print('No message received.')
            return 'EVENT_RECEIVED', 200
        else:
          print("Key 'messaging' not found in the dictionary.")
          return 'EVENT_RECEIVED', 200
    else:
      pass
      return 'ERROR', 404


# Function to start a new event loop in a separate thread
def start_background_loop(loop):
  asyncio.set_event_loop(loop)
  loop.run_forever()


# Create an independent event loop
background_loop = asyncio.new_event_loop()
threading.Thread(target=start_background_loop,
                 args=(background_loop, ),
                 daemon=True).start()

# Ensure your background task runs on this new loop
asyncio.run_coroutine_threadsafe(
    handle_message.process_message_queue_after_delay(None), background_loop)

if __name__ == '__main__':
  # Start background task
  loop = asyncio.get_event_loop()
  loop.create_task(handle_message.process_message_queue_after_delay(None))

  # Start the Flask server
  from waitress import serve
  serve(app, host='0.0.0.0', port=3000, threads=12, connection_limit=1000)
