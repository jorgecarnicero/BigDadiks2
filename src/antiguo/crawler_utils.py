# crawler_utils.py
import time
import boto3

def start_crawler_and_wait(crawler_name: str, wait: bool = True, poll_seconds: int = 15) -> None:
    """
    Arranca un crawler existente. Si ya está RUNNING/STOPPING, no hace nada.
    Si wait=True, espera hasta que termine.
    """
    glue = boto3.client("glue")

    # Verifica que existe (si no existe, lanzará excepción)
    glue.get_crawler(Name=crawler_name)

    state = glue.get_crawler(Name=crawler_name)["Crawler"]["State"]  # READY | RUNNING | STOPPING
    if state != "READY":
        # Ya está ejecutándose o parando; evitamos error por start duplicado
        return

    glue.start_crawler(Name=crawler_name)

    if not wait:
        return

    while True:
        state = glue.get_crawler(Name=crawler_name)["Crawler"]["State"]
        if state == "READY":
            break
        time.sleep(poll_seconds)
