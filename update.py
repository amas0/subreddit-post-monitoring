import os
import time
from pathlib import Path

import db
import submission

SUBREDDIT = 'dndnext'
NEW_SUBMISSIONS_LIMIT = 20
AGE_OFF_SECONDS = 172800  # 48 hours

client_id = os.environ.get('REDDIT_CLIENT_ID')
client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
db_path = Path(os.environ.get('MONITORING_DB_PATH'))

if __name__ == '__main__':
    reddit = submission.create_client(client_id, client_secret)

    with db.MonitoringDB(db_path=db_path) as mdb:
        subs, stats = submission.get_new_submissions(SUBREDDIT, reddit, limit=NEW_SUBMISSIONS_LIMIT)
        for sub, stat in zip(subs, stats):
            mdb.insert_submission(sub)
            mdb.insert_stats(stat)

        time_now = int(time.time())
        old_monitored_submissions = mdb.get_recent_submissions(time_now - AGE_OFF_SECONDS)
        # Don't double update posts still in new
        submissions_to_update = set(old_monitored_submissions) - set(subs)
        new_stats = [submission.get_submission_stats_from_id(sub.id, client=reddit)
                     for sub in submissions_to_update]
        for stat in new_stats:
            mdb.insert_stats(stat)
