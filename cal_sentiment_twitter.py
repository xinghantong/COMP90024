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

def readTwitterFile(argv):
    #load the file in json, but for the large one should read line by line
    twitter_json = open(argv)
    structure = json.load(twitter_json)
    print(structure)
    # twitter_json.readline()
    # nextLine = twitter_json.readline()[:-2]
    # line_ex = json.loads(nextLine)
    # print(line_ex["value"])




if __name__ == "__main__":
    #enter the line in terminal to run it
    #python cal_sentiment_twitter.py AFINN.txt tinyTwitter.json
    main(sys.argv[1])
    readTwitterFile(sys.argv[2])