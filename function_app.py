import os, json, logging, datetime as dt
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()   # <-- no minus sign

def get_secret(name: str) -> str:
    kv_name = os.environ["KEYVAULT_NAME"]
    vault_url = f"https://{kv_name}.vault.azure.net"
    client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
    return client.get_secret(name).value

def get_container_client(container: str):
    conn = os.environ["AzureWebJobsStorage"]
    return BlobServiceClient.from_connection_string(conn).get_container_client(container)

@app.function_name(name="TimerIngest")
@app.timer_trigger(schedule="0 0 7 * * *", arg_name="myTimer",
                   run_on_startup=False, use_monitor=True)
def TimerIngest(myTimer: func.TimerRequest):
    logging.info("TimerIngest fired.")
    # KV smoke test
    _ = get_secret("REED-API-KEY")
    _ = get_secret("POSTGRES-CONN-STRING")
    logging.info("Fetched Key Vault secrets OK.")

    # Write a tiny blob
    now = dt.datetime.utcnow()
    blob = f"{now:%Y}/{now:%m}/jobs-{now:%Y%m%d-%H%M%S}.json"
    payload = {"ok": True, "utc_time": now.isoformat()+"Z"}
    get_container_client(os.getenv("BLOB_CONTAINER", "raw-jobs")) \
        .upload_blob(name=blob, data=json.dumps(payload), overwrite=True)
    logging.info(f"Wrote blob: {blob}")