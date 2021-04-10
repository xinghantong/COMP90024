import sys
import json
from mpi4py import MPI
import time
import re


def main(argv):
    #create a dictionary for matching the word
    word_file = open(str(argv),'r')
    #print(str(argv))
    word_dict = {}
    for word_info in word_file:
        #print(word_info)
        word_info = word_info.strip().split('\t')
        #print(word_info)
        word_dict[word_info[0]] = word_info[1]
    return word_dict

#load and parse melbGrid file
def generateMelbGrid(argv):
    melb_grid = open(argv)
    melb_grid_json = json.load(melb_grid)
    x_axis = []
    y_axis = []
    x_name = ["1","2","3","4","5"]
    y_name = ["D","C","B","A"]
    feature = melb_grid_json["features"]
    for f in feature:
        if f["properties"]["xmin"] not in x_axis:
            x_axis.append(f["properties"]["xmin"])
        if f["properties"]["xmax"] not in x_axis:
            x_axis.append(f["properties"]["xmax"])
        if f["properties"]["ymin"] not in y_axis:
            y_axis.append(f["properties"]["ymin"])
        if f["properties"]["ymax"] not in y_axis:
            y_axis.append(f["properties"]["ymax"])
    x_axis.sort()
    y_axis.sort()
    #slightlt move the min x_ais and y_axis to consider the edge cases, which helps to locate the tweets
    x_axis[0] -= 1e-8
    y_axis[0] -= 1e-8

    return [x_axis,y_axis,x_name,y_name]


def readTwitterFile(argv,grid,word_dict):
    #load the file in json, but for the large one should read line by line

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    start_time = time.time()
    grid_score = {}
    grid_count = {}

    #initialise grid_count and grid_score
    x_axis, y_axis, x_name, y_name = grid
    for x in x_name:
        for y in y_name:
            grid_score[y + x] = 0
            grid_count[y + x] = 0
    for key in ['A5','B5','D1','D2']:
        if key in grid_count:
            del grid_count[key]
            del grid_score[key]

    #each process open and read tweet file line by line
    with open(argv, encoding='utf-8') as tweet_file:
        basic_info = tweet_file.readline()
        basic_info = basic_info[:-10] + '}'
        basic_info_json = json.loads(basic_info)
        
        #skip first several tweets by process's rank
        for i in range(rank):
            next(tweet_file)
        while 1:
            try:
                #match the coordinates and text in each tweet using regex
                line = tweet_file.readline()
                m = re.search('"coordinates":\[([\d.-]+),([\d.-]+)\]', line)                
                tweet_pos = [float(m.group(1)),float(m.group(2))]
                s = re.search('"text":"([\s\S]*?)"', line)
                tweet_text = s.group(1)
                countScore(word_dict, gird, tweet_pos, tweet_text, grid_score,grid_count)
                
                #each process will read one line every size-1 lines
                for i in range(size-1):
                    try:
                        next(tweet_file)
                    except:
                        pass

            except:
                break

   
    #gather generic object from each process to root by using gather
    grid_score = comm.gather(grid_score, root=0)
    grid_count = comm.gather(grid_count,root=0)

    #show result in process 0
    if rank == 0:
        total_score = {}
        for score in grid_score:
            for key in score.keys():
                if key in total_score:
                    total_score[key] += score[key]
                else:
                    total_score[key] = score[key]
        total_count = {}

        for count in grid_count:
            for key in count.keys():
                if key in total_count:
                    total_count[key] += count[key]
                else:
                    total_count[key] = count[key]

        sort_score = sorted(total_score.items(), key=lambda x: x[1], reverse=True)
        print("{0:8s} {1:<8s} {2:<8s}".format("Cell","#Tweets","#Sentiment Score"))
        for key,value in sort_score:
            print("{0:8s} {1:<8d} {2:<8d}".format(key,total_count[key],value))
        print("{0:8s} {1:<8d} {2:<8d}".format("total",sum(total_count.values()),sum(total_score.values())))
        print(time.time()-start_time)

#locate the index of x_ais and y_ais, note that target is strictly larger than the border(not equal),
#so the edge cases will locate to the grid on the left and below.
def findIndex(nums,target):
    compared_num = nums[0]
    index = 0
    while target > compared_num:
        index+=1
        compared_num = nums[index]
    return index-1

#calculate grid_count and grid_score
def countScore(word_dict, melbGrid, tweet_pos, tweet_text,grid_score,grid_count):
    x_axis, y_axis, x_name, y_name = melbGrid
    invalid_area = ['A5','B5','D1','D2']
    #the coordinate is strictly larger than its left bottom border, smaller or equals to its right top border
    if tweet_pos[0]> x_axis[0] and tweet_pos[0] <= x_axis[-1] and tweet_pos[1]> y_axis[0] and tweet_pos[1] <= y_axis[-1]:
        x = findIndex(x_axis, tweet_pos[0])
        y = findIndex(y_axis, tweet_pos[1])
        #find the cell's id
        area = y_name[y] + x_name[x]
        #return if invalid area, not need to count
        if area in invalid_area:
            return
        grid_count[area] += 1
        
        #split the textline into words by whitespace characters using regex, all specical cases in AFINN file are excluded.
        text_line = re.findall(r'\S*\b(?:can\'t stand|cashing in|cool stuff|does not work|dont like|fed up|green wash|green washing|messing up|no fun|not good|not working|right direction|screwed up|some kind)\b|\S+', tweet_text.lower())
        for token in text_line:
            #split by given punctuations
            res = re.split(r'[.,?!\'\"]+', token)
            for x in res:
                if x and x in word_dict:
                    grid_score[area] += int(word_dict[x])

    return grid_score


'''
this function is to count the score based the word sentiment, melb grid and tweet text and pos
'''

if __name__ == "__main__":
    #enter the line in terminal to run it
    #python cal_sentiment_twitter.py AFINN.txt tinyTwitter.json melbGrid2.json
    gird = generateMelbGrid(sys.argv[3])
    word_dict = main(sys.argv[1])
    readTwitterFile(sys.argv[2],gird,word_dict)

