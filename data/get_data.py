import time
import csv
import praw
import re

reddit = praw.Reddit(client_id='jIrXdcl2J6vsjw',
                     client_secret='N6eypRrFBJikJht595x0T75yLyM',
                     password='s89-zd6-BJn-oNo',
                     user_agent='scrapbot by /u/edoggee',
                     username='calpolystudent42069')

print("Start: ", time.strftime("%Y-%m-%d %H:%M:%S"))

with open('subs-raw.txt', 'r') as f:
    CONTENT = f.readlines()

TOP_SUBS = []
for line in CONTENT:
    if line.startswith('/r/', ):
        TOP_SUBS.append(line[3:].split(' ')[0].strip())

data = open('reddit.csv', 'w')
writer = csv.writer(data, delimiter=',')

writer.writerow(['id', 'subreddit', 'score', 'title', 'timeCreated', 'numComments', 'subscribers'])

for ndx, sub in enumerate(TOP_SUBS):
    subs = reddit.subreddit(sub).subscribers

    for i, submission in enumerate(reddit.subreddit(sub).top(limit=1000)):
        votes = submission.score
        sub = submission.subreddit
        title = re.sub(r"[^a-zA-Z0-9]+", ' ', submission.title.strip().replace(',', '').replace('"', '').replace('\n', '').strip())
        time_created = submission.created
        num_comments = submission.num_comments

        writer.writerow([submission, sub, votes, title, time_created, num_comments, subs])

        if i % 100 == 0:
            print((str(int((float(i + 1) / 1000.0) * 100))) + "% done with " + str(sub))

    print("Finished " + str(sub) + ". " + str(ndx + 1) + " done")


print("Finish: ", time.strftime("%Y-%m-%d %H:%M:%S"))
