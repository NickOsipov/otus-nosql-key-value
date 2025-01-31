"""
Simple pipe with cache
"""

import os
from datetime import datetime

from loguru import logger
from redis import Redis
from sqlalchemy import create_engine, text

REDIS_CLIENT = Redis(host="localhost", port=6379, db=0, decode_responses=True)
CH_CLIENT = create_engine("clickhouse+native://default@localhost:9099/default")

QUERY = """
SELECT user_id, name, email, registration_date
FROM default.users_info
WHERE toDate(registration_date) = today()
"""

if __name__ == "__main__":
    logger.info("Start pipeline")
    with CH_CLIENT.connect() as conn:
        users = conn.execute(text(QUERY)).fetchall()
        logger.info(f"Found {len(users)} users")

    dt_now = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join("reports", f"report_{dt_now}.txt")

    with open(report_path, "w", encoding="utf-8") as f:
        for user in users:
            logger.debug(f"Processing user {user.user_id}")
            user_id = user.user_id
            user_name = user.name
            user_email = user.email

            if not REDIS_CLIENT.exists(f"user:{user_id}"):
                logger.debug(f"User {user_id} not found in cache")
                f.write(f"id: {user_id}\nname: {user_name}\nemail: {user_email}\n---\n")
                mapping = {"id": user_id, "name": user_name, "email": user_email}
                REDIS_CLIENT.hset(f"user:{user_id}", mapping=mapping)
                logger.info(f"User {user_id} added to cache")
            else:
                logger.debug(f"User {user_id} found in cache")

    logger.info("Pipeline finished")
    logger.info(f"Report saved to {report_path}")
