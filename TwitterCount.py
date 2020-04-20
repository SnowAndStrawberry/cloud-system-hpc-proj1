"""
Author:Dongfang Wang 906257 dongfangw@student.unimelb.edu.au
       Suryadi Tjandra 1112164 shtjandra@student.unimelb.edu.au
Project: COMP90024 - Assignment 1
Purpose: The purpose in this programming assignment is to implement
           a simple, parallelized application leveraging the University
           of Melbourne HPC facility SPARTAN
"""
from mpi4py import MPI
import json

#Use the mpi4py module and get the state.
class Mpi:
    def __init__(self):
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()


#Define the data structure of Twitter, get the attribution needed.
class Twitter:
    def __init__(self, line):
        if line.get('doc').get('entities').get('hashtags'):
            self.hashtag = [each.get('text').lower() for each in line.get('doc').get('entities').get('hashtags')]
        else:
            self.hashtag = None
        if line.get('doc').get('lang'):
            self.lang = line.get('doc').get('lang')
        else:
            self.lang = None


#A help function to Load the data and assign the work to different core.
def process(mpi,file):
    hashtagList = {}
    languageList = {}
    lineNumber = 0
    with open(file, encoding='utf-8') as load_f:
        for line in load_f:
            lineNumber += 1
            if mpi.rank == lineNumber % mpi.size:
                try:
                    line = json.loads(line[:-2])
                except Exception:
                    continue
                twitter = Twitter(line)
                if twitter.hashtag:
                    hashtag = twitter.hashtag
                    for tag in hashtag:
                        hashtagList[tag] = hashtagList.get(tag, 0) + 1
                if twitter.lang:
                    lang = twitter.lang
                    languageList[lang] = languageList.get(lang, 0) + 1
    return hashtagList, languageList


#A help function to rank the top 10 languages and hashtags based on their number.
def rank(rankList,rankNumber):
    ranked = sorted(rankList.items(), key=lambda items: items[1], reverse=True)
    return ranked[:rankNumber]


#A help function to combine the data.
def combine(uncombinedData):
    combinedData = {}
    for each in uncombinedData:
        for k, v in each.items():
            combinedData[k] = combinedData.get(k, 0) + v
    return combinedData


#The dictionary of language abbreviations
languageDic = {
    'am':'Amharic',
    'ar':'Arabic',
    'hy':'Armenian',
    'bn':'Bengali',
    'bg':'Bulgarian',
    'my':'Burmese',
    'zh':'Chinese',
    'cs':'Czech',
    'da':'Danish',
    'nl':'Dutch',
    'en':'English',
    'et':'Estonian',
    'fi':'Finnish',
    'fr':'French',
    'ka':'Georgian',
    'de':'German',
    'el':'Greek',
    'gu':'Gujarati',
    'ht':'Haitian',
    'iw':'Hebrew',
    'hi':'Hindi',
    'hu':'Hungarian',
    'is':'Icelandic',
    'in':'Indonesian',
    'it':'Italian',
    'ja':'Japanese',
    'kn':'Kannada',
    'km':'Khmer',
    'ko':'Korean',
    'lo':'Lao',
    'lv':'Latvian',
    'lt':'Lithuanian',
    'ml':'Malayalam',
    'dv':'Maldivian',
    'mr':'Marathi',
    'ne':'Nepali',
    'no':'Norwegian',
    'or':'Oriya',
    'pa':'Panjabi',
    'ps':'Pashto',
    'fa':'Persian',
    'pl':'Polish',
    'pt':'Portuguese',
    'ro':'Romanian',
    'ru':'Russian',
    'sr':'Serbian',
    'sd':'Sindhi',
    'si':'Sinhala',
    'sk':'Slovak',
    'sl':'Slovenian',
    'ckb':'Sorani Kurdish',
    'es':'Spanish',
    'sv':'Swedish',
    'tl':'Tagalog',
    'ta':'Tamil',
    'te':'Telugu',
    'th':'Thai',
    'bo':'Tibetan',
    'tr':'Turkish',
    'uk':'Ukrainian',
    'ur':'Urdu',
    'ug':'Uyghur',
    'vi':'Vietnamese',
    'cy':'Welsh',
    'und':'Undefined'
}


#Gather the data processed in different core.
#The main class to print the result.
if __name__ == "__main__":
    data = "bigTwitter.json"
    RankNumber = 10
    mpi = Mpi()
    hashtagList, languageList = process(mpi, data)
    hashtagList = mpi.comm.gather(hashtagList, root=0)
    langList = mpi.comm.gather(languageList, root=0)
    if mpi.rank == 0:
        combinedHashtagList = combine(hashtagList)
        combinedLanguageList = combine(langList)
        topHashtags = rank(combinedHashtagList,RankNumber)
        topLangs = rank(combinedLanguageList,RankNumber)
        print('-----In data', data, ':-----')
        print("-----The top 10 hashtags are:-----")
        for i, hashtag in enumerate(topHashtags):
            print(str(i + 1) + ". #" + hashtag[0] + ", " + str(hashtag[1]))
        print("\n")
        print("-----The top 10 languages used are:-----")
        for j, language in enumerate(topLangs):
            print(str(j + 1) + ". " + languageDic[language[0]] + "(" + language[0] + "), " + str(language[1]))
