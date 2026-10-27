import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

with sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
    http_path=os.environ["DATABRICKS_HTTP_PATH"],
    access_token=os.environ["DATABRICKS_TOKEN"],
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT current_catalog(), current_user()")
        print(cur.fetchall())