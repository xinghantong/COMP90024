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






def readTwitterFile(argv):
    #load the file in json, but for the large one should read line by line

    tweet_text = []
    tweet_pos = []
    with open(argv) as tweet_file:
        basic_info = tweet_file.readline()
        basic_info = basic_info[:-10]+'}'
        basic_info_json = json.loads(basic_info)
        row = basic_info_json['total_rows']
        for i in range(row-2):

            line = tweet_file.readline()
            # print(line[-2])
            tweet_dict = json.loads(line[:-2])
            tweet_pos.append(tweet_dict['value']['geometry']['coordinates'])
            tweet_text.append(tweet_dict["doc"]["text"])



#def countScore(word_dict, melbGrid, tweet_pos, tweet_text):
'''
this function is to count the score based the word sentiment, melb grid and tweet text and pos
'''

if __name__ == "__main__":
    #enter the line in terminal to run it
    #python cal_sentiment_twitter.py AFINN.txt tinyTwitter.json melbGrid2.json
    main(sys.argv[1])
    readTwitterFile(sys.argv[2])
    generateMelbGrid(sys.argv[3])