from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime
import json
import os.path
import time
"""
Download Gmail messages using the Gmail API.

Authenticates, then retrieves all messages from the user's inbox (using pagination, batched),
and writes them to a json lines file.
"""


CREDENTIALS_FILE_PATH = ".auth_cache/credentials.json"
TOKEN_FILE_PATH = "auth_cache/token.json"
OUTPUT_FILE_FOLDER = "output"
OUTPUT_FILE_PATH = OUTPUT_FILE_FOLDER + "/output.jsonl"
AUTH_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def get_credentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN_FILE_PATH):
        creds = Credentials.from_authorized_user_file(
            TOKEN_FILE_PATH, AUTH_SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE_PATH, AUTH_SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_FILE_PATH, "w") as token:
            token.write(creds.to_json())

    return creds


def get_messages(service, user_id, label_ids, max_results, callback):
    ''' list and get messages using batch and paginate, call callback for each batch returned '''

    messages_service = service.users().messages()
    list_request = messages_service.list(userId=user_id,
                                         labelIds=label_ids, maxResults=max_results)
    total_messages_received = 0
    start_time = time.time()
    while list_request is not None:
        list_request_executed = list_request.execute()
        messages = list_request_executed.get("messages", [])
        total_messages_received += len(messages)
        print(f"{datetime.now().strftime('%H:%M:%S')}: Got {len(messages)} messages" +
              f" (total={total_messages_received}, elapsed_time={time.time() - start_time:.2f}s)")

        all_msgs = []

        def batch_callback(request_id, response, exception):
            if exception is not None:
                print(f"Error for request {request_id}: {exception}")
            else:
                all_msgs.append(response)

        get_batch_request = service.new_batch_http_request(
            callback=batch_callback)
        for message in messages:
            get_batch_request.add(
                service.users().messages().get(
                    userId=user_id, id=message["id"])
            )
        get_batch_request.execute()
        callback(all_msgs)

        list_request = messages_service.list_next(
            list_request, list_request_executed)


def main():
    try:
        creds = get_credentials()
        service = build("gmail", "v1", credentials=creds)

        os.makedirs(OUTPUT_FILE_FOLDER, exist_ok=True)
        with open(OUTPUT_FILE_PATH, "w", encoding='utf8') as f:
            def callback(messages):
                for message in messages:
                    f.write(json.dumps(message, ensure_ascii=False) + "\n")

            # 20 seems to be the max for concurrent limits (enforced in batch too)
            get_messages(service, "me", None, 20, callback)

    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f"An error occurred: {error}")


if __name__ == "__main__":
    main()
