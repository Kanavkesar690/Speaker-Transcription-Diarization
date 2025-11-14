import os
import json
import time
import tempfile
from dotenv import load_dotenv
import pyodbc
import azure.cognitiveservices.speech as speechsdk
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobClient

# Load environment variables
load_dotenv()

# Speech service configuration
speech_key = os.getenv("SPEECH_KEY")
speech_region = os.getenv("SPEECH_REGION")

# Queries
GET_FILES_QUERY = os.getenv("GET_FILES_QUERY")
INSERT_TRANSCRIPT_QUERY = os.getenv("INSERT_TRANSCRIPT_QUERY")
UPDATE_FILE_QUERY = os.getenv("UPDATE_FILE_QUERY")

# Database connection helper
def databaseConn(query, params=None):
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    user_name = os.getenv('USER_NAME')
    password = os.getenv('PASSWORD')
    
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={user_name};'
        f'PWD={password};'
    )
 
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    try:
        if params:
            cursor.execute(query, params)
            cnxn.commit()
            return cursor
        else:
            cursor.execute(query)        
            return cursor
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise


# Fetch unprocessed files
def fetch_unprocessed_files():
    cursor = databaseConn(GET_FILES_QUERY)
    rows = cursor.fetchall()
    cursor.close()
    return rows

# Insert transcript into database
def insert_transcript(doc_id, transcript):
    databaseConn(INSERT_TRANSCRIPT_QUERY, (doc_id, transcript))

# Mark file as processed
def mark_file_processed(doc_id):
    databaseConn(UPDATE_FILE_QUERY, (doc_id,))

# Speech to Text with Diarization
def speech_to_text_diarize(doc_id, context) -> str:
    payload = json.loads(context) if isinstance(context, str) else context
    audio_url = payload.get("arguments", {}).get("audio_url")

    if not audio_url:
        return json.dumps({"error": "Missing 'audio_url' argument"})
    if not audio_url.lower().endswith(".wav"):
        return json.dumps({"error": "Only .wav files are accepted"})

    # Download using Managed Identity
    try:
        blob_client = BlobClient.from_blob_url(blob_url=audio_url, credential=ManagedIdentityCredential(client_id=os.getenv("UAMI_CLIENT_ID")))
        tmp_file = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
        with open(tmp_file.name, "wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())
        wav_path = tmp_file.name
    except Exception as e:
        return json.dumps({"error": f"Failed to download blob via managed identity: {e}"})

    if not speech_key or not speech_region:
        os.remove(wav_path)
        return json.dumps({"error": "SPEECH_KEY or SPEECH_REGION not set"})

    speech_config = speechsdk.SpeechConfig(
        subscription=speech_key,
        region=speech_region
    )
    speech_config.output_format = speechsdk.OutputFormat.Detailed

    audio_config = speechsdk.audio.AudioConfig(filename=wav_path)
    transcriber = speechsdk.transcription.ConversationTranscriber(
        speech_config=speech_config,
        audio_config=audio_config
    )

    segments = []
    done = False

    # Event handler for transcribed results
    def on_transcribed(evt):
        nonlocal segments
        if evt.result.reason == speechsdk.ResultReason.RecognizedSpeech:
            detail = json.loads(evt.result.json)
            speaker_id = detail.get("SpeakerId", "Unknown")
            text = detail.get("DisplayText", evt.result.text)

            # Skip empty or whitespace-only results
            if text and text.strip():
                segments.append({f"Speaker {speaker_id}": text})
                print(f"Captured turn for DocID {doc_id}: Speaker {speaker_id} -> {text}")

    # Event handler for session stopped or canceled
    def on_stop(evt):
        nonlocal done
        done = True

    transcriber.transcribed.connect(on_transcribed)
    transcriber.session_stopped.connect(on_stop)
    transcriber.canceled.connect(on_stop)

    transcriber.start_transcribing_async()
    while not done:
        time.sleep(0.5)
    transcriber.stop_transcribing_async()

    try:
        os.remove(wav_path)
    except OSError:
        pass

    if not segments:
        return json.dumps({"error": "No speech recognized"})

    # Insert once with the full transcript (all segments)
    segments_json = json.dumps(segments)
    insert_transcript(doc_id, segments_json)

    return segments_json

# Main function to process all unprocessed files
def process_unprocessed_files():
    files = fetch_unprocessed_files()
    if not files:
        print("No unprocessed files found.")
        return
    
    for doc_id, blob_url in files:
        print(f"Processing DocID {doc_id}: {blob_url}")
        context = {"arguments": {"audio_url": blob_url}}
        result = speech_to_text_diarize(doc_id, context)

        # After inserting transcript, mark file as processed
        mark_file_processed(doc_id)
        print(f"DocID {doc_id} fully processed and marked complete.")

