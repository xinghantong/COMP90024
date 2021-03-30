import sys
import json
from mpi4py import MPI
import time


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


def generateMelbGrid(argv):
    melb_grid = open(argv)
    melb_grid_json = json.load(melb_grid)
    x_axis = []
    y_axis = []
    x_name = ["1","2","3","4","5"]
    y_name = ["A","B","C","D"]
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

    return [x_axis,y_axis,x_name,y_name]






def readTwitterFile(argv,grid,word_dict):
    #load the file in json, but for the large one should read line by line

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    start_time = time.time()
    with open(argv) as tweet_file:
        basic_info = tweet_file.readline()
        basic_info = basic_info[:-10] + '}'
        basic_info_json = json.loads(basic_info)
        grid_score = {}
        numth = 0
        for i in range(rank):
            tweet_file.readline()
            numth +=1
            # print("skiphead")
        while 1:
            try:
                tweet_pos = []
                tweet_text = []
                line = tweet_file.readline()
                tweet_dict = json.loads(line[:-2])
                tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
                tweet_text.append(tweet_dict["doc"]["text"])
                grid_score = countScore(word_dict, gird, tweet_pos, tweet_text, grid_score)
                # print(numth,grid_score,"this is process:",rank)
                numth += 1
                for i in range(size-1):
                    try:
                        tweet_file.readline()
                        numth += 1
                    except:
                        return

            except:
                for i in range(len(line)):
                    try:
                        tweet_dict = json.loads(line[:len(line) - i])
                        tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
                        tweet_text.append(tweet_dict["doc"]["text"])
                        grid_score = countScore(word_dict, gird, tweet_pos, tweet_text, grid_score)
                        numth+=1
                        break
                    except:
                        pass
                break
    print(numth, grid_score, "this is process:", rank)
    grid_score = comm.gather(grid_score, root=0)
    if rank == 0:
        print(numth, grid_score, "this is process:", rank)
        total_score = {}
        for score in grid_score:
            for key in score.keys():
                if key in total_score:
                    total_score[key] += score[key]
                else:
                    total_score[key] = score[key]
        print(total_score)
        print(time.time()-start_time)

def searchInsert(nums, target):
    left = 0
    right = len(nums) - 1
    while left <= right:
        mid = (right + left) // 2
        if target == nums[mid]:
            return mid, True
        elif target < nums[mid]:
            right = mid - 1
        else:
            left = mid + 1
    return right

def countScore(word_dict, melbGrid, tweet_pos, tweet_text,grid_score):
    x_axis, y_axis, x_name, y_name = melbGrid
    #print(tweet_pos)
    punctuation = [",", ".", "\'", "\"", "?", "!"]
    for i in range(len(tweet_pos)):
        x = searchInsert(x_axis,tweet_pos[i][0])
        y = searchInsert(y_axis,tweet_pos[i][1])
        #print(x_axis,y_axis,tweet_pos[i])
        area = y_name[len(y_name)-1-y] + x_name[x]
        if area not in grid_score:
            grid_score[area] = 0

        text_line = tweet_text[i]
        text_line = text_line.strip().split(' ')
        #print(text_line)
        score = 0
        for text in text_line:
            text = text.lower()
            if len(text)>0 and text[-1] in punctuation:
                text = text[:-1]
            if text in word_dict:

                grid_score[area] += int(word_dict[text])
                score += int(word_dict[text])
        #print(i,score)

    #print(tweet_text)
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

