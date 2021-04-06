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

    return [x_axis,y_axis,x_name,y_name]






def readTwitterFile(argv,grid,word_dict):
    #load the file in json, but for the large one should read line by line

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    start_time = time.time()
    grid_score = {}
    grid_count = {}
    x_axis, y_axis, x_name, y_name = grid
    for x in x_name:
        for y in y_name:
            grid_score[y + x] = 0
            grid_count[y + x] = 0
    for key in ['A5','B5','D1','D2']:
        if key in grid_count:
            del grid_count[key]
            del grid_score[key]

    with open(argv, encoding='utf-8') as tweet_file:
        basic_info = tweet_file.readline()
        basic_info = basic_info[:-10] + '}'
        basic_info_json = json.loads(basic_info)

        for i in range(rank):
            # tweet_file.readline()
            next(tweet_file)
            #numth +=1
            # print("skiphead")
        while 1:
            try:
                line = tweet_file.readline()

                # tweet_dict = json.loads(line[:-2])
                # tweet_pos = tweet_dict['value']['geometry']['coordinates']
                # tweet_text = tweet_dict['value']['properties']["text"]
                m = re.search('"coordinates":\[([\d.-]+),([\d.-]+)\]', line)                
                tweet_pos = [float(m.group(1)),float(m.group(2))]
                s = re.search('"text":"([\s\S]*?)"', line)
                tweet_text = s.group(1)
                countScore(word_dict, gird, tweet_pos, tweet_text, grid_score,grid_count)
                # numth += 1
                for i in range(size-1):
                    try:
                        # tweet_file.readline()
                        next(tweet_file)
                        # numth += 1
                    except:
                        # print(numth)
                        pass

            except:
                break


                # for i in range(len(line)):
                #     try:
                #         # tweet_dict = json.loads(line[:len(line) - i])
                #         # tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
                #         # tweet_text.append(tweet_dict['value']['properties']["text"])
                #         m = re.search('"coordinates":\[([\d.-]+),([\d.-]+)\]', line)                
                #         tweet_pos = [float(m.group(1)),float(m.group(2))]
                #         s = re.search('"text":"([\s\S]*?)"', line)
                #         tweet_text = s.group(1)
                #         countScore(word_dict, gird, tweet_pos, tweet_text, grid_score,grid_count)
                #         grid_score = countScore(word_dict, gird, tweet_pos, tweet_text, grid_score)
                #         # numth+=1
                #         break
                #     except:
                #         pass

                
    #print(numth, grid_score, "this is process:", rank)
    grid_score = comm.gather(grid_score, root=0)
    grid_count = comm.gather(grid_count,root=0)
    if rank == 0:
        #print(numth, grid_score, "this is process:", rank)
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
        print((sort_score))
        print("{0:8s} {1:<8s} {2:<8s}".format("Cell","#Tweets","#Sentiment Score"))
        for key,value in sort_score:
            print("{0:8s} {1:<8d} {2:<8d}".format(key,total_count[key],value))
        print("{0:8s} {1:<8d} {2:<8d}".format("total",sum(total_count.values()),sum(total_score.values())))

        #print(total_score)
        #print(total_count)
        print(time.time()-start_time)


def findIndex(nums,target):
    compared_num = nums[0]
    index = 0
    while target >= compared_num:
        index+=1
        compared_num = nums[index]
    return index-1


def countScore(word_dict, melbGrid, tweet_pos, tweet_text,grid_score,grid_count):
    x_axis, y_axis, x_name, y_name = melbGrid
    #print(tweet_pos)
    invalid_area = ['A5','B5','D1','D2']
    punctuation = [",", ".", "\'", "\"", "?", "!"]
    if tweet_pos[0]>= x_axis[0] and tweet_pos[0] <= x_axis[-1] and tweet_pos[1]>= y_axis[0] and tweet_pos[1] <= y_axis[-1]:
        x = findIndex(x_axis, tweet_pos[0])
        y = findIndex(y_axis, tweet_pos[1])
        area = y_name[y] + x_name[x]
        #print(area, y, x, tweet_pos)
        if area in invalid_area:
            return
        # if area not in grid_count:
        #     grid_count[area] = 1
        # else:
        #     grid_count[area] += 1
        # if area not in grid_score:
        #     grid_score[area] = 0
        grid_count[area] += 1
        
        # print(text_line)
        #score = 0

        # 1
        # for text in text_line:
        #     text = text.lower()
        #     if len(text) > 0 and text[-1] in punctuation:
        #         text = text[:-1]
        #     if text in word_dict:
        #         grid_score[area] += int(word_dict[text])
        #         #score += int(word_dict[text])

        #2
        # text_line = tweet_text.strip().split(' ')
        # for token in text_line:
        #     token = token.lower()
        #     token = re.sub(r'([\?\!\.\,\'\"])+$', "", token)
        #     if token in word_dict:
        #         grid_score[area] += int(word_dict[token])

        #3
        text_line = re.findall(r'\S*\b(?:can\'t stand|cashing in|cool stuff|does not work|dont like|fed up|green wash|green washing|messing up|no fun|not good|not working|right direction|screwed up|some kind)\b|\S+', tweet_text.lower())
        for token in text_line:
            res = re.split(r'(?![can\'t stand])[.,?!\'\"]+', token)
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

