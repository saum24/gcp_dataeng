
from base64 import b64decode
import logging
from pandas import DataFrame
from json import loads
from google.cloud.storage import Client

class LoadToStorage:
    def __init__(self ,event, context):
        self.event = event
        self.context = context
        self.bucket_name = "saums_bucket"

    def get_message_data(self) -> str:
        logging.info(
            f"This Funtion was triggered by messageId {self.context.event_id} published at {self.context.timestamp}"
            f"to {self.context.resource['name']}"
        )

        if "data" in self.event:
            pubsub_message = b64decode(self.event['data']).decode('utf-8')
            print(pubsub_message)
            logging.info(pubsub_message)
            return pubsub_message
        else:
            logging.error("Incorrect format")
            return ""

    def structure_payload(self ,message: str) -> DataFrame:
        try:
            df = DataFrame(loads(message))
            if not df.empty():
                logging.info(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
            else:
                logging.warning(f"Created empty DataFrame")
            return df
        except Exception as e:
            logging.error(f"Encountered error creating DataFrame - {str(e)}")
            raise

    def upload_to_bucket(self, df: DataFrame, file_name: str = "payload") -> None:

        storage_client = Client()
        bucket = storage.client.bucket(self.bucket_name)
        df.to_csv('Sample.csv' ,index=False)
        blob = bucket.blob('samplefile.csv')
        blob.upload_from_filename('Sample.csv')
        # blob.upload_from_string(data=df.to_csv(index=False), content_type= "text/csv")
        logging.info(f"File uploaded to {self.bucket_name}")


def process(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    svc = LoadToStorage(event, context)

    message = svc.get_message_data()
    print(message)
    upload_df = svc.structure_payload(message)
    payload_timestamp = upload_df["price_timestamp"].unique().tolist()[0]

    svc.upload_to_bucket(upload_df, "Crypto_data_" + payload_timestamp)
