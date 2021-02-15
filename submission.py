import time
from dataclasses import dataclass

import praw


@dataclass(frozen=True)
class Submission:
    id: str
    author: str
    created_utc: int
    name: str
    permalink: str
    selftext: str
    title: str

    @classmethod
    def from_submission(cls, sub: praw.reddit.Submission):
        return cls(
            id=sub.id,
            author=sub.author.name,
            created_utc=int(sub.created_utc),
            name=sub.name,
            permalink=sub.permalink,
            selftext=sub.selftext,
            title=sub.title
        )


@dataclass(frozen=True)
class SubmissionStats:
    submission_id: str
    time_utc: int
    ups: int
    downs: int
    num_comments: int

    @classmethod
    def from_submission(cls, sub: praw.reddit.Submission, time_utc: int):
        return cls(
            submission_id=sub.id,
            time_utc=time_utc,
            ups=int(sub.ups),
            downs=int(sub.downs),
            num_comments=int(sub.num_comments)
        )


def create_client(client_id: str, client_secret: str):
    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=f'linux:{client_id}:v0.1.0 (by /u/_amas_)'
    )


def get_new_submissions(subreddit: str, client: praw.Reddit,
                        limit: int = 10) -> tuple[list[Submission], list[SubmissionStats]]:
    metas, temporal_data = [], []
    for sub in client.subreddit(subreddit).new(limit=limit):
        time_utc = int(time.time())
        metas.append(Submission.from_submission(sub))
        temporal_data.append(SubmissionStats.from_submission(sub, time_utc))
    return metas, temporal_data


def get_temp_data_from_id(submission_id: str, client: praw.Reddit) -> SubmissionStats:
    sub = client.submission(submission_id)
    time_utc = int(time.time())
    return SubmissionStats.from_submission(sub, time_utc)
