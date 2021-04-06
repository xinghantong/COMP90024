#!/usr/bin/env python
# coding=utf-8
import json
from mpi4py import MPI
import re
import time

start_time = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def load_sentiment_dict(dict_path):
    res = {}
    with open(dict_path,"r") as f:
        for line in f.readlines():
            a, b = line.strip().split('\t')
            res[a] = int(b)
        return res


def load_melb_grid(grid_path):
    x_axis = set()
    y_axis = set()
    x_name = ["1","2","3","4","5"]
    y_name = ["D","C","B","A"]
    with open(grid_path,"r", encoding='utf-8') as f:
        data = json.load(f)
        for x in data['features']:
            x_axis.add(x['properties']["xmin"])
            x_axis.add(x['properties']["xmax"])
            y_axis.add(x['properties']["ymin"])
            y_axis.add(x['properties']["ymax"])
        x_axis = list(x_axis)
        y_axis = list(y_axis)
        x_axis.sort()
        y_axis.sort()
        x_axis[0] -= 1e-8
        y_axis[0] -= 1e-8
    return [x_axis,y_axis,x_name,y_name]


def parse_line(line, grids):
    m = re.search('"coordinates":\[([\d.-]+),([\d.-]+)\]', line)
    if m and m.group(1) and m.group(2):
        point = [float(m.group(1)),float(m.group(2))]
        grid_id = compare_grid(point,grids)
        if grid_id and grid_id not in invalid_area:
            s = re.search('"text":"([\s\S]*?)"', line)
            if s and s.group(1):
                sentiment = cal_sentiment(s.group(1))
                return grid_id, sentiment
    return None, None


def find_index(nums,target):
    compared_num = nums[0]
    index = 0
    while target > compared_num:
        index+=1
        compared_num = nums[index]
    return index-1
        

def compare_grid(point, grids):
    x_axis, y_axis, x_name, y_name = grids
    if point[0]>x_axis[0] and point[0]<=x_axis[-1] and point[1]>y_axis[0] and point[1]<=y_axis[-1]:
        x = find_index(x_axis, point[0])
        y = find_index(y_axis, point[1])
        return y_name[y] + x_name[x]
    else:
        return None


def cal_sentiment(text):
    score = 0
    text_line = re.findall(r'\S*\b(?:can\'t stand|cashing in|cool stuff|does not work|dont like|fed up|green wash|green washing|messing up|no fun|not good|not working|right direction|screwed up|some kind)\b|\S+', text.lower())
    for token in text_line:
        res = re.split(r'(?![can\'t stand])[.,?!\'\"]+', token)
        for x in res:
            if x and x in sentiment_dict:
                score += sentiment_dict[x]
    return score


GridPath = "melbGrid2.json"
InstaPath = "smallTwitter.json"
SentimentPath = "AFINN.txt"

grids = load_melb_grid(GridPath)
sentiment_dict = load_sentiment_dict(SentimentPath)

grid_sentiment = {}
grid_count = {}

x_axis, y_axis, x_name, y_name = grids
for x in x_name:
    for y in y_name:
        grid_sentiment[y + x] = 0
        grid_count[y + x] = 0

invalid_area = ['A5','B5','D1','D2']
for area in invalid_area:
    del grid_count[area]
    del grid_sentiment[area]

with open(InstaPath, "r", encoding='utf-8') as f:
    line_num = 0
    for line in f:
        line_num += 1
        if (line_num % size) == rank:
            grid_id, sentiment = parse_line(line, grids)
            if grid_id:
                grid_sentiment[grid_id] += sentiment
                grid_count[grid_id] += 1

grid_sentiment = comm.gather(grid_sentiment, root=0)
grid_count = comm.gather(grid_count, root=0)

comm.Barrier()

if comm.rank == 0:
    sentiment = {}
    count = {}

    for p_dict in grid_sentiment:
        for (k,v) in p_dict.items():
            if not sentiment.get(k):
                sentiment[k] = 0
            sentiment[k] += v

    for p_dict in grid_count:
        for (k,v) in p_dict.items():
            if not count.get(k):
                count[k] = 0
            count[k] += v

    sorted_sentiment = sorted(sentiment.items(), key=lambda x:x[1], reverse=True)
    print("Cell         #Total Tweets       #Overal Sentiment Score")
    for cell in sorted_sentiment:
        print(' {0:8} {1:10,} {2:+25}'.format(cell[0], count[cell[0]], cell[1]))

    print("{0:8s} {1:<8d} {2:<8d}".format("total",sum(count.values()),sum(sentiment.values())))
    print("Used time:",time.time() - start_time,"(s)")
