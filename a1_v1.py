#!/usr/bin/env python
# coding=utf-8
import json
from mpi4py import MPI
import re
import time

start_time = time.time()
comm = MPI.COMM_WORLD

def load_sentiment_dict(dict_path):
    res = {}
    with open(dict_path,"r") as f:
        for line in f.readlines():
            a, b = line.strip().split('\t')
            res[a] = int(b)
        return res

def load_melb_grid(grid_path):
    res = []
    with open(grid_path,"r") as f:
        data = json.load(f)
        for x in data['features']:
            if 'properties' in x:
                res.append(x['properties'])
    return res

def parse_line(line, grids):
    m = re.search('"coordinates":\[([\d.-]+),([\d.-]+)\]', line)
    if m and m.group(1) and m.group(2):
        point = [float(m.group(1)),float(m.group(2))]
        grid_id = compare_grid(point,grids)
        if grid_id:
            s = re.search('"text":"([\s\S]*?)"', line)
            if s and s.group(1):
                sentiment = cal_sentiment(s.group(1))
                return grid_id, sentiment
    return None, None

def compare_grid(point, grids):
    for grid in grids:
        if is_in_grid(point,grid):
            return grid['id']
    return None

def is_in_grid(point, grid):
    px, py = point
    if px > grid['xmin'] and px <= grid['xmax'] and py > grid['ymin'] and py <= grid['ymax']:
        return True
    else:
        return False

def cal_sentiment(text):
    score = 0
    tokens = re.split('\s',text.lower())
    for token in tokens:
        if token in sentiment_dict:
            score += sentiment_dict[token]
    return score

GridPath = "melbGrid2.json"
InstaPath = "smallTwitter.json"
SentimentPath = "AFINN.txt"

grids = load_melb_grid(GridPath)
sentiment_dict = load_sentiment_dict(SentimentPath)

grid_sentiment = {}
grid_count = {}

for grid in grids:
    grid_sentiment[grid['id']] = 0
    grid_count[grid['id']] = 0

with open(InstaPath, "r", encoding='utf-8') as f:
    line_num = 0
    for line in f:
        line_num += 1
        if (line_num % comm.size) == comm.rank:
            grid_id, sentiment = parse_line(line, grids)
            if grid_id:
                grid_sentiment[grid_id] += sentiment
                grid_count[grid_id] += 1

comm.Barrier()

data = comm.gather(grid_sentiment)
print("Cell         #Total Tweets       #Overal Sentiment Score")

for cell in grids:
    print(' {0:8} {1:10,} {2:+25,}'.format(cell['id'], grid_count[cell['id']], grid_sentiment[cell['id']]))

print("Time:",time.time() - start_time,"(sec)")
