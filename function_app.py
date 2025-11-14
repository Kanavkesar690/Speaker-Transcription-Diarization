import azure.functions as func
from TranscriptionFile import process_unprocessed_files
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Azure Function HTTP trigger for Speech to Text Transcription
@app.route(route="SpeechToTextTranscription")
def SpeechToTextTranscription(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function started.')

    try:
        run = process_unprocessed_files()
        logging.info('Run function executed successfully.')
        return func.HttpResponse(
            "Function executed successfully.",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error while executing function: {e}")
        return func.HttpResponse(
            f"Function failed with error: {str(e)}",
            status_code=500
        )