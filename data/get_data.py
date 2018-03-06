import time
import csv
import praw


reddit = praw.Reddit(client_id='jIrXdcl2J6vsjw',
                     client_secret='N6eypRrFBJikJht595x0T75yLyM',
                     password='s89-zd6-BJn-oNo',
                     user_agent='scrapbot by /u/edoggee',
                     username='calpolystudent42069')

# tops = reddit.subreddit('all').top()

# for top in tops:
    # print(praw.models.Submission(reddit=reddit,id=top))

count = 0
print("Start: ", time.strftime("%Y-%m-%d %H:%M:%S"))


data = open('reddit.csv', 'w')
writer = csv.writer(data, delimiter=',')

writer.writerow(['id', 'score', 'subreddit', 'title', 'timeCreated', 'numComments'])

for submission in reddit.front.top(limit=10000):
    count += 1
    if count % 100 == 0:
        print(count)

    votes = submission.score
    sub = submission.subreddit
    title = submission.title
    time_created = submission.created
    num_comments = submission.num_comments

    writer.writerow([submission, votes, sub, title, time_created, num_comments])

print("Finish: ", time.strftime("%Y-%m-%d %H:%M:%S"))
