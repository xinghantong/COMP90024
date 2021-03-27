import sys
import json

def main(argv):
    #create a dictionary for matching the word
    word_file = open(str(argv),'r')
    print(str(argv))
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

    tweet_text = []
    tweet_pos = []
    with open(argv) as tweet_file:
        basic_info = tweet_file.readline()
        basic_info = basic_info[:-10]+'}'
        basic_info_json = json.loads(basic_info)
        row = basic_info_json['total_rows']
        print(row)

        while 1:
            try:
                line = tweet_file.readline()
                tweet_dict = json.loads(line[:-2])
                tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
                tweet_text.append(tweet_dict["doc"]["text"])
            except:
                for i in range(len(line)):
                    try:
                        tweet_dict = json.loads(line[:len(line)-i])
                        tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
                        tweet_text.append(tweet_dict["doc"]["text"])
                        break
                    except:
                        pass
                break
        #print(len(tweet_pos))



        #     tweet_dict = json.loads(line[:-2])
        #     tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
        #     tweet_text.append(tweet_dict["doc"]["text"])
        # line = tweet_file.readline()
        # tweet_dict = json.loads(line[:-3])
        # tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
        # tweet_text.append(tweet_dict["doc"]["text"])
        socre_dict = countScore(word_dict,gird,tweet_pos,tweet_text)
        # line = tweet_file.readline()
        # print(line[-3:])

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

def countScore(word_dict, melbGrid, tweet_pos, tweet_text):
    x_axis, y_axis, x_name, y_name = melbGrid
    grid_score = {}
    punctuation = "! , ? . ’ ”"
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
    print(grid_score)









'''
this function is to count the score based the word sentiment, melb grid and tweet text and pos
'''

if __name__ == "__main__":
    #enter the line in terminal to run it
    #python cal_sentiment_twitter.py AFINN.txt tinyTwitter.json melbGrid2.json
    gird = generateMelbGrid(sys.argv[3])
    word_dict = main(sys.argv[1])
    readTwitterFile(sys.argv[2],gird,word_dict)

