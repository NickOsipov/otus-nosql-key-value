"""
Script: src/pipeline.py
"""

import os
from datetime import datetime

from loguru import logger
from sqlalchemy import create_engine, text
from redis import Redis


REDIS_CLIENT = Redis(host="localhost", port=6380, db=0)
CH_ENGINE = create_engine("clickhouse+native://default:password@localhost:9099/default")

QUERY = """
SELECT user_id, name, email
FROM users_info
WHERE toDate(registration_date) = today()
"""

def main():
    logger.info("Starting data extraction pipeline")

    logger.info("Connecting to ClickHouse and executing query")
    with CH_ENGINE.connect() as conn:
        users = conn.execute(text(QUERY)).fetchall()

    dt_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join("reports", f"report_{dt_now}.txt")

    with open(report_path, "w") as report_file:
        for user in users:
            user_id = user[0]
            name = user[1]
            email = user[2]

            if not REDIS_CLIENT.exists(f"user:{user_id}"):
                report_file.write(f"id: {user_id}\nname: {name}\nemail: {email}\n")
                mapping_data = {
                    "name": name,
                    "email": email
                }
                REDIS_CLIENT.hset(f"user:{user_id}", mapping=mapping_data)
                logger.info(f"Processed user_id: {user_id}")
            else:
                logger.info(f"User_id: {user_id} already exists in Redis, skipping.")

    logger.info(f"Report generated at {report_path}")


if __name__ == "__main__":
    main()
