# Author: Dustin Doubet
# Description: Find if two words can be found to be an anagram
# Python 2.7
# 
# Requirements:
#
# No 3rd party libs or special functions
#
# Case 1: Find if the words are an anagram: 'Cat' and 'Act'
# Case 2: Apply for words: 'Caat' and 'Aact'
# Case 3: Use only one dictionary and performance optimization is important
# 
# Unknown: Not sure if white space is allowed which would produce a phrase and not a word
#

import sys
from time import time


def getCharDict(word):
    """ Creates a dictionary of unique character keys with each
        character count as the value.
        { 'c' : 1, 'a' : 2, 't' : 1}
    """
    charDict = {}
    for item in word:
        if item in charDict.keys():
            charDict[item] += 1
        else:
            charDict[item] = 1            
    return charDict

def compareCharMaps(dict1, dict2):
    """ Uses two dictionaries one for each word and then 
        compares the two by short circuit breaking if there
        is a character in one not in the other. Otherwise,
        no short circuit then no miss-matches.
    """
    isAnagram = False
    for key in dict1.keys():
        if dict1[key] != dict2[key]:
            break
    else:
        isAnagram = True
    return isAnagram

def compareCharMapsAlt(dict1, dict2):
    """ Dictionary to dictionary comparision
        dict1 == dict2 is not a address comparision
    """
    return dict1 == dict2

def countCharInWord(char, word):
    count = 0
    for _char in word:
        if _char == char:
            count +=1
    return count
        
def compareCharMapWordCount(charDict, word):
    """Only uses one dictionary for input to compare
       against the 2nd input word but applies a count
       on the word for possibly each character. This
       works but is not the most efficient.
       
       I included this because this was what I was
       the 2nd solution that I was working through
       that we decided that the count was not performant
       enough.
    """
    isAnagram = False
    for char in word:
        if (char not in charDict.keys() or 
           charDict[char] != word.count(char)):
            break
    else:
        isAnagram = True
    return isAnagram

def compareCharMapToWord(charDict, word):
    """Only uses one dictionary for input to compare
       against the 2nd input word.
       
       This is an alternate solution to 
       compareCharMapWordCount
    """
    isAnagram = False
    charDictKeys = charDict.keys()
    for char in word:
        if (char not in charDictKeys):
            break
        else:
            charDict[char] += 1
    else:
        # The else would run if no breaks and makes sure there
        # are no unnecesary conditional checks before the return
        #
        # Because we are checking whether the char exists
        # in the dictionary we know at this point that the word
        # has the same characters as the first word and because
        # we are evaluating two words, if there is a mismatch on
        # characters across them then this would produce an odd count
        for key in charDictKeys:
            if charDict[key] % 2 != 0:
                break
        else:
            isAnagram = True

    return isAnagram

def compareCharMapToWord3(charDict, word):
    """Only uses one dictionary for input to compare
       against the 2nd input word but applies a count
       on the word for possibly each character. This
       works but is not the most efficient.
       
       I included this because this was what I was
       the 2nd solution that I was working through
       that we decided that the count was not performant
       enough.
    """
    isAnagram = False
    for char in word:
        charCount = 0
        if (char not in charDict.keys() and 
           charDict[char] != word.count(char)):
            break
        else:
            charCount += 1
    else:
        isAnagram = True
    return isAnagram

def sortCompare(word1, word2):
    """ Only if sorted is allowed ?
        Sorted list to sorted list comparision
        which like the dictionary to dictionary
        comparision is not an address comparision.
    """
    return sorted(word1) == sorted(word2)

def main(input1, input2):
    word1 = input1.lower(); print "Word 1: " + word1
    word2 = input2.lower(); print "Word 2: " + word2
    
    noMsg = "\nThese two words are not an anagram"
    yesMsg = "These two words: %s, %s are an anagram"
    
    if (len(word1) != len(word2) or
        # If the set collection is allowed then each set would give
        # the unique characters of it's word and then then length can 
        # be compared.
        len(set(word1)) != len(set(word2))):
        print noMsg                              
    else:
        # At this point we would know the words have the same length
        # and that they have the same unique character set.
                                
        # 1st approach with 2 dictionaries, one for each word. Not the 
        # most efficient.
        t1 = time()
        if compareCharMaps(getCharDict(word1), getCharDict(word2)):
            print "\nMethod 1: " + yesMsg%(input1,input2)
        else:
            print noMsg
        t2 = time()
        print "Method 1: Verification time: " + str(t2 - t1)
        t2 = time()
        if compareCharMapsAlt(getCharDict(word1), getCharDict(word2)):
            print "\nMethod 2: " + yesMsg%(input1,input2)
        else:
            print noMsg
        t3 = time()
        print "Method 2: Verification time: " + str(t3 - t2)
        t3 = time()
        # Approach using only one dictionary and input word with no word.count
        if compareCharMapToWord(getCharDict(word1), word2):
            print "\nMethod 3: " + yesMsg%(input1,input2)        
        else:
            print noMsg
        t4 = time()
        print "Method 3: Verification time: " + str(t4 - t3)
        t4 = time()
        if compareCharMapWordCount(getCharDict(word1), word2):
            print "\nMethod 4: " + yesMsg%(input1,input2)        
        else:
            print noMsg
        t5 = time()
        print "Method 4: Verification time: " + str(t5 - t4)
        t5 = time()
        if sortCompare(word1, word2):
            print "\nMethod 5: " + yesMsg%(input1,input2)        
        else:
            print noMsg
        t6 = time()
        print "Method 5: Verification time: " + str(t6 - t5)
    
if __name__ == "__main__":
    input1 = sys.argv[1]
    input2 = sys.argv[2]
    main(input1,input2)

# input1 = "Caaat"
# input2 = "Aaact"
# main(input1,input2)
