import time
import csv
import praw


TOP_SUBS = ['announcements', 'funny', 'AskReddit', 'todayilearned', 'science', 'worldnews', 'pics', 'IAmA', 'gaming', 'videos', 'movies', 'aww', 'Music', 'blog', 'gifs', 'news', 'explainlikeimfive', 'askscience', 'EarthPorn', 'books', 'television', 'mildlyinteresting', 'LifeProTips', 'Showerthoughts', 'space', 'DIY', 'Jokes', 'gadgets', 'nottheonion', 'sports', 'tifu', 'food', 'photoshopbattles', 'Documentaries', 'Futurology', 'history', 'InternetIsBeautiful', 'dataisbeautiful', 'UpliftingNews', 'listentothis', 'GetMotivated', 'personalfinance', 'OldSchoolCool', 'philosophy', 'Art', 'nosleep', 'WritingPrompts', 'creepy', 'TwoXChromosomes', 'Fitness', 'technology', 'WTF', 'bestof', 'AdviceAnimals', 'politics', 'atheism', 'interestingasfuck', 'europe', 'woahdude', 'BlackPeopleTwitter', 'oddlysatisfying', 'gonewild', 'leagueoflegends', 'pcmasterrace', 'reactiongifs', 'gameofthrones', 'wholesomememes', 'Unexpected', 'Overwatch', 'facepalm', 'trees', 'Android', 'lifehacks', 'me_irl', 'relationships', 'Games', 'nba', 'programming', 'tattoos', 'NatureIsFuckingLit', 'Whatcouldgowrong', 'CrappyDesign', 'dankmemes', 'nsfw', 'cringepics', '4chan', 'soccer', 'comics', 'sex', 'pokemon', 'malefashionadvice', 'NSFW_GIF', 'StarWars', 'Frugal', 'HistoryPorn', 'AnimalsBeingJerks', 'RealGirls', 'travel', 'buildapc', 'OutOfTheLoop']

reddit = praw.Reddit(client_id='jIrXdcl2J6vsjw',
                     client_secret='N6eypRrFBJikJht595x0T75yLyM',
                     password='s89-zd6-BJn-oNo',
                     user_agent='scrapbot by /u/edoggee',
                     username='calpolystudent42069')

print("Start: ", time.strftime("%Y-%m-%d %H:%M:%S"))

data = open('reddit.csv', 'w')
writer = csv.writer(data, delimiter=',')

writer.writerow(['id', 'subreddit', 'score', 'title', 'timeCreated', 'numComments'])

for ndx, sub in enumerate(TOP_SUBS):
    for i, submission in enumerate(reddit.subreddit(sub).top(limit=1000)):
        votes = submission.score
        sub = submission.subreddit
        title = submission.title
        time_created = submission.created
        num_comments = submission.num_comments

        writer.writerow([submission, sub, votes, title, time_created, num_comments])

        if i % 100 == 0:
            print((str(int((float(i + 1) / 1000.0) * 100))) + "% done with " + str(sub))

    print("Finished " + str(sub) + ". " + str(ndx + 1) + " done")


print("Finish: ", time.strftime("%Y-%m-%d %H:%M:%S"))
