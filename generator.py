from random import randint, gauss
from statistics import median

num_users=10000
num_clips=1000
num_ratings=10000

deviation = 2

target_score = {clip: randint(1, 10) for clip in range(1, num_clips + 1)}
score = {clip: 0 for clip in range(1, num_clips + 1)}
count = {clip: 0 for clip in range(1, num_clips + 1)}
ratings = {}
clip_ratings = {clip: [] for clip in range(1, num_clips + 1)}

output_file = 'input.txt'

with open(output_file, 'w') as f:
    for i in range(num_ratings):
        user = randint(1, num_users)
        clip = randint(1, num_clips)
        rating = gauss(target_score[clip], deviation)
        rating = round(min(max(rating, 1), 10))
        score[clip] += rating
        count[clip] += 1
        ratings[(user, clip)] = rating
        clip_ratings[clip].append(rating)
        # print(user, clip, rating)
        f.write('{};{};{}\n'.format(user, clip, rating))

medians = {clip: 0 if not clip_ratings[clip] else median(clip_ratings[clip]) for clip in range(1, num_clips + 1)}

for k, v in ratings.items():
    u, c = k
    if v == 1 or v == 10:
        if count[c] > 0:
            clip_mean = score[c] / count[c]
            if abs(v - medians[c]) > 5:
                print('user: {}, clip: {}, rating: {}, median: {}, mean: {}'.format(u, c, v, medians[c], clip_mean))
