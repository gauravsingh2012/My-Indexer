import string
import math
import sys

def generateListOfStopWords():
    listOfStopWords = []
    stopWordsPath = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/stoplist.txt"
    queryFile = open(stopWordsPath, "r")
    for line in queryFile:
        listOfStopWords.append(line.strip())
    return listOfStopWords

def queryFromFile():
    path = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/query_desc.51-100.short.txt"
    queryFile = open(path, "r")
    listOfStopWords = generateListOfStopWords()
    combinedDict = {}
    for line in queryFile:
        line = line.replace('-', " ")
        for c in string.punctuation:
            line = line.replace(c,"")
        queryTFDict = {}
        queryTerms = line.split()
        for x in range(0, 3):
            queryTerms.pop(1)
        
        queryTermsWithoutStopWords = [term for term in queryTerms if term not in listOfStopWords]
        
        for term in queryTermsWithoutStopWords:
            if term.startswith('"') and term.endswith('"'):
                term = term[1:-1]
            if queryTFDict.has_key(term):
                
                queryTFDict[term] = queryTFDict[term] + 1
            else:    
                queryTFDict[term] = 1
        
        print queryTermsWithoutStopWords
        
        combinedDict = queryElasticSearch(queryTermsWithoutStopWords, queryTFDict)
        #queryElasticSearchForSmoothing(queryTermsWithoutStopWords, queryTFDict)
        
        #queryElasticSearchForJelinek(queryTermsWithoutStopWords, queryTFDict)
        
        queryTermsWithoutStopWords = [term for term in queryTerms if term not in listOfStopWords]
        
        for term in queryTermsWithoutStopWords:
            if term.startswith('"') and term.endswith('"'):
                term = term[1:-1]
            if queryTFDict.has_key(term):
                
                queryTFDict[term] = queryTFDict[term] + 1
            else:    
                queryTFDict[term] = 1

        proximitySearchModel(queryTermsWithoutStopWords, queryTFDict, combinedDict)

def readDictToMemory(dict,filename):
    file = open(filename, 'r')
    for line in file:
        splitLine = line.split()
        dict[splitLine[0]] = int (splitLine[1]) 
    return dict

def readCatalogToMemory(dict,filename):
    file = open(filename, 'r')
    for line in file:
        splitLine = line.split()
        dict[int(splitLine[0])] = splitLine[1] 
    return dict

def readDocIdToMemory(dict,filename):
    file = open(filename, 'r')
    for line in file:
        splitLine = line.split()
        dict[int(splitLine[1])] = splitLine[0] 
    return dict

def queryElasticSearch(queryTermsWithoutStopWords, queryTFDict):
    queryNo = queryTermsWithoutStopWords.pop(0)
    invertedListFile = open("F:/work/Workspaces/Test/index/invertedList167.txt", 'r')
    lengthDict = storeLengthOfDocumentsInDictionary()
    termIdMap = {}
    docIdMap = {}
    catalogMap = {}
    tfList = []
    dict = {}    
    termTfMap={}
        
    termIdMap = readDictToMemory(termIdMap, "termIdMap.txt")
    docIdMap = readDocIdToMemory(docIdMap, "docIdMap.txt")
    catalogMap = readCatalogToMemory(catalogMap, "F:/work/Workspaces/Test/index/catalog/catalog167.txt")
    
    print queryTermsWithoutStopWords
    
    for eachTerm in queryTermsWithoutStopWords:
        eachTerm1 = eachTerm 
        termTfMap={}
        if eachTerm.startswith('"') and eachTerm.endswith('"'):
            eachTerm = eachTerm[1:-1]
        
        if (eachTerm == 'US'):
            eachTerm  = 'u.s'
            
        termId = termIdMap.get(eachTerm.lower())
        
        catalogOffset = catalogMap.get(termId)
        
        if (catalogOffset is not None):
            splitOffset = catalogOffset.split(',')
            invertedListFile.seek(long(splitOffset[0]))
            termFrequencyList = invertedListFile.read(long(splitOffset[1]) - 2 - long(splitOffset[0]))
            resultList = termFrequencyList.split(',')
            resultList.pop(0)
            
            for result in resultList:
                docIdTfList = result.split()
                documentId = docIdTfList.pop(0)
                actualDocId = docIdMap.get(int(documentId))
                termTfMap[actualDocId] = docIdTfList
        
        print float(len(termTfMap))
        for key in termTfMap:
            if (key is not None):
                #okapiTfForAllTerms (float(len(termTfMap.get(key))) , 452.780152888 , float(lengthDict[str(key)]) , str(key), dict)
                #tfIdforAllTerms (float(len(termTfMap.get(key))) , 452.780152888 , float(lengthDict[str(key)]) , str(key) , dict, float(len(termTfMap)))   
                okapiBm25 (float(len(termTfMap.get(key))) , 452.780152888 , float(lengthDict[str(key)]) , str(key) , dict, float(len(termTfMap)), float(queryTFDict[eachTerm1]))
               
        print eachTerm + " Finished"
    #writeQueryModelsToFile(dict, queryNo,"okapibm25MyIndexer.txt",500)
    return dict
    print queryNo + " Finished"
    
def queryElasticSearchForSmoothing(queryTermsWithoutStopWords, queryTFDict):
    queryNo = queryTermsWithoutStopWords.pop(0)
    invertedListFile = open("F:/work/Workspaces/Test/index/invertedList167.txt", 'r')
    print queryNo
    lengthDict = storeLengthOfDocumentsInDictionary()
    tfList = []
    listOfDicts=[]
    termIdMap = {}
    docIdMap = {}
    catalogMap = {}
    smoothingDict = storeDictionaryIds()
    
    termIdMap = readDictToMemory(termIdMap, "termIdMap.txt")
    docIdMap = readDocIdToMemory(docIdMap, "docIdMap.txt")
    catalogMap = readCatalogToMemory(catalogMap, "F:/work/Workspaces/Test/index/catalog/catalog167.txt")
    
    
    print queryTermsWithoutStopWords
    for eachTerm in queryTermsWithoutStopWords:
        dict = {}
        eachTerm1 = eachTerm 
        termTfMap={}
        if eachTerm.startswith('"') and eachTerm.endswith('"'):
            eachTerm = eachTerm[1:-1]
       
        if (eachTerm == 'US'):
            eachTerm  = 'u.s'
            
        termId = termIdMap.get(eachTerm.lower())
        
        catalogOffset = catalogMap.get(termId)
        
        if (catalogOffset is not None):
            splitOffset = catalogOffset.split(',')
            invertedListFile.seek(long(splitOffset[0]))
            termFrequencyList = invertedListFile.read(long(splitOffset[1]) - 2 - long(splitOffset[0]))
            resultList = termFrequencyList.split(',')
            resultList.pop(0)
            
            for result in resultList:
                docIdTfList = result.split()
                documentId = docIdTfList.pop(0)
                actualDocId = docIdMap.get(int(documentId))
                termTfMap[actualDocId] = docIdTfList
        
        print float(len(termTfMap))
        for key in termTfMap:
            if (key is not None):
                unigramLMLaplaceSmoothing(float(len(termTfMap.get(key))) ,  202073 ,float(lengthDict[str(key)]) , str(key), dict)

        listOfDicts.append(dict)
        
        print eachTerm + " Finished"
    
    for key in smoothingDict:
        for eachDict in listOfDicts:
            if eachDict.has_key(key):
                smoothingDict[key] = smoothingDict[key] + eachDict[key]
            else:
                p_laplace = 1.0 / (lengthDict[key] + 202073)
                lm_lapace_zero = math.log10(p_laplace)
                smoothingDict[key] = smoothingDict[key] + lm_lapace_zero
    
    writeQueryModelsToFile(smoothingDict, queryNo, "laplaceSmoothingMyIndexer.txt",1000)
    print queryNo + " Finished"        
    
def queryElasticSearchForJelinek(queryTermsWithoutStopWords, queryTFDict):
    queryNo = queryTermsWithoutStopWords.pop(0)
    invertedListFile = open("F:/work/Workspaces/Test/index/invertedList167.txt", 'r')
    lengthDict = storeLengthOfDocumentsInDictionary()
    tfList = []
    ttfDict = {}
    listOfDicts=[]
    smoothingDict = storeDictionaryIds()
    termIdMap = {}
    docIdMap = {}
    catalogMap = {}
    
    termIdMap = readDictToMemory(termIdMap, "termIdMap.txt")
    docIdMap = readDocIdToMemory(docIdMap, "docIdMap.txt")
    catalogMap = readCatalogToMemory(catalogMap, "F:/work/Workspaces/Test/index/catalog/catalog167.txt")
    
    
    print queryTermsWithoutStopWords
    for eachTerm in queryTermsWithoutStopWords:
        dict = {}
        eachTerm1 = eachTerm 
        termTfMap={}
        if eachTerm.startswith('"') and eachTerm.endswith('"'):
            eachTerm = eachTerm[1:-1]
        
        if (eachTerm == 'US'):
            eachTerm  = 'u.s'
            
        termId = termIdMap.get(eachTerm.lower())
        
        catalogOffset = catalogMap.get(termId)
        
        if (catalogOffset is not None):
            splitOffset = catalogOffset.split(',')
            invertedListFile.seek(long(splitOffset[0]))
            termFrequencyList = invertedListFile.read(long(splitOffset[1]) - 2 - long(splitOffset[0]))
            resultList = termFrequencyList.split(',')
            resultList.pop(0)
            
            for result in resultList:
                docIdTfList = result.split()
                documentId = docIdTfList.pop(0)
                actualDocId = docIdMap.get(int(documentId))
                termTfMap[actualDocId] = docIdTfList
        
        print float(len(termTfMap))
        
        ttf = 0
        
        for key in termTfMap:
            if (key is not None):
                ttf = ttf + len (termTfMap.get(key)) 

        ttfDict[eachTerm] = float(ttf)             

        for key in termTfMap:
            if (key is not None):        
                unigramLMJelinek(float(len(termTfMap.get(key))),13953998,float(lengthDict[str(key)]),str(key), dict, ttf, eachTerm)
        
        listOfDicts.append(dict)
        print eachTerm + " Finished"
    
    for key in smoothingDict:
        for eachDict in listOfDicts:
            if eachDict:
                if eachDict.values()[0].has_key(key):
                    value = eachDict.values()[0]
                    smoothingDict[key] = smoothingDict[key] + value[key]
                else:
                    termKey = eachDict.keys()[0]
                    p_jm = 0.5 *( ttfDict[termKey] / 13953998)
                    lm_jm_zero = math.log10(p_jm)
                    smoothingDict[key] = smoothingDict[key] + lm_jm_zero
    
    writeQueryModelsToFile(smoothingDict, queryNo, "jelinekMercerSmoothingMyIndexer500.txt",1000)
    print queryNo + " Finished"
    
def writeQueryModelsToFile(dict, queryNo, filename,topDocuments):
    f = open(filename , "a")
    i = 1
    for doc in sorted(dict, key=dict.get, reverse=True):
        qrelString = queryNo + " Q0 " + doc + " " + str(i) + " " + str(dict[doc]) + " " + "Exp" 
        f.write(qrelString + "\n")
        if (i == topDocuments):
            break
        i = i + 1
        
def okapiBm25(tf, avgLengthDocument, lengthDocument, docId, dict, df, tfq):
    k1 = 1.2
    k2 = 100.0
    b = 0.75
    
    c1 = math.log10((84678 + 0.5) / (df + 0.5))
    c2 = (tf + (k1 * tf)) / (tf + k1 * ((1 - b) + (b * (lengthDocument / avgLengthDocument))))
    c3 = (tfq + (k2 * tfq)) / tfq + k2
    
    bm25 = c1 * c2 * c3
    
    if (dict.has_key(docId)):
        dict[docId] = dict[docId] + bm25
    else:
        dict[docId] = bm25

def okapiTfForAllTerms(tf, avgLengthDocument, lengthDocument, docId, dict):
    
    denom = tf + 0.5 + (1.5 * (lengthDocument / avgLengthDocument))
    okapiTfForATerm = tf / denom
    
    if (dict.has_key(docId)):
        dict[docId] = dict[docId] + okapiTfForATerm
    else:
        dict[docId] = okapiTfForATerm

def tfIdforAllTerms(tf, avgLengthDocument, lengthDocument, docId, dict, df):

    okapiTfForATerm = tf / (tf + 0.5 + 1.5 * (lengthDocument / avgLengthDocument))
    
    tfIdf = okapiTfForATerm * math.log10(84678 / df) 
    
    if (dict.has_key(docId)):
        tfIdfScore = dict.get(docId)
        newTfIdfScore = tfIdfScore + tfIdf
        dict[docId] = newTfIdfScore
    else:
        dict[docId] = tfIdf

def unigramLMLaplaceSmoothing(tf, vocabulary, lengthDocument, docId, dict):
    p_laplace = (tf + 1.0) / (float(lengthDocument) + vocabulary)
    lm_laplace = math.log10(p_laplace)
    dict[docId] = lm_laplace

def unigramLMJelinek(tf,totalDocumentLength,lengthDocument,docId,dict,ttf,eachTerm):
    l = 0.5
    c1 = l * (tf / lengthDocument)
    c2 = (ttf - tf) / (totalDocumentLength - lengthDocument)
    p_jm = c1 + ((1 - l)*c2)
    lm_jm = math.log10(p_jm)
    if dict.has_key(eachTerm):
        dict[eachTerm][docId] = lm_jm
    else:
        dict[eachTerm] = {eachTerm : lm_jm}
        
def findLengthOfAllDocumentFromElasticSearch():
    sum = 0
    dict = storeLengthOfDocumentsInDictionary()
    print len(dict)
    for key in dict:
        length = dict.get(key) 
        sum = sum + length
        
    print sum / 84768 
    
def storeLengthOfDocumentsInDictionary():
    file = open("lengthOfDocuments.txt", "r")
    dict = {}
    for line in file:
        listOfLengthDocuments = line.split();
        dict[listOfLengthDocuments[0]] = float(listOfLengthDocuments[1])
    return dict

def storeDictionaryIds():
    file = open("lengthOfDocuments.txt", "r")
    dict = {}
    for line in file:
        listOfLengthDocuments = line.split();
        dict[listOfLengthDocuments[0]] = 0.0
    return dict

def proximitySearchModel(queryTermsWithoutStopWords, queryTFDict, combinedDict):
    queryNo = queryTermsWithoutStopWords.pop(0)
    invertedListFile = open("F:/work/Workspaces/Test/index/invertedList167.txt", 'r')
    lengthDict = storeLengthOfDocumentsInDictionary()
    termIdMap = {}
    docIdMap = {}
    catalogMap = {}
    tfList = []
    dict = {}    
    termTfMap={}
    
    proximityDict = {}
        
    termIdMap = readDictToMemory(termIdMap, "termIdMap.txt")
    docIdMap = readDocIdToMemory(docIdMap, "docIdMap.txt")
    catalogMap = readCatalogToMemory(catalogMap, "F:/work/Workspaces/Test/index/catalog/catalog167.txt")
    
    print queryTermsWithoutStopWords
    
    for eachTerm in queryTermsWithoutStopWords:
        listOfStemmedTerm = []
        eachTerm1 = eachTerm 
        if eachTerm.startswith('"') and eachTerm.endswith('"'):
            eachTerm = eachTerm[1:-1]
        
        if (eachTerm == 'US'):
            eachTerm  = 'u.s'
            
        stemmedTerm = eachTerm.lower()    
        
        termId = termIdMap.get(stemmedTerm)
        catalogOffset = catalogMap.get(termId)
        
        if (catalogOffset is not None):
            splitOffset = catalogOffset.split(',')
            invertedListFile.seek(long(splitOffset[0]))
            termFrequencyList = invertedListFile.read(long(splitOffset[1]) - 2 - long(splitOffset[0]))
            resultList = termFrequencyList.split(',')
            resultList.pop(0)
                
            for result in resultList:
                docIdTfList = result.split()
                documentId = docIdTfList.pop(0)
                actualDocId = docIdMap.get(int(documentId))
                if termTfMap.has_key(actualDocId) :
                    if (termTfMap.get(actualDocId).has_key(stemmedTerm)):
                        list1 = termTfMap.get(actualDocId).get(stemmedTerm)
                        #print str(actualDocId) + " " + str(termTfMap[actualDocId][stemmedTerm]) + " " + str(list1)
                        termTfMap[actualDocId][stemmedTerm] = list1 + docIdTfList
                    else:
                        termTfMap[actualDocId][stemmedTerm] = docIdTfList
                else:
                    termTfMap[actualDocId] = {} 
                    termTfMap[actualDocId][stemmedTerm] = docIdTfList
                    #print str(actualDocId) + " " + str(termTfMap[actualDocId][stemmedTerm]) + " " + str(docIdTfList)
        print float(len(termTfMap))                   
                    
        print eachTerm + " Finished"
    
    
    for key in termTfMap:
            numberOfTerms = len (termTfMap.get(key))
            if (key is not None):
                    minRange = sys.maxint
                    termsDictionary = termTfMap.get(key)
                    termListPositions = []
                    for keyTerm in termsDictionary:
                        termListPositions.append(termsDictionary[keyTerm])
                    
                    window = []
                    for termList in termListPositions:
                        if (len(termList) == 0):
                            break
                        window.append(termList[0])

                    print window

                    while True:   
                        if len(window) == 0:
                            break
                        if (len(window) == 1):
                            break
                        minValue = int(min(window))
                        
                        maxValue = int(max(window))
                        print maxValue
                        print minValue
                        range = math.fabs(maxValue - minValue)
                        print range
                        minIndex = window.index(str(minValue))
                        window.pop(minIndex)
                        insertNextElement = termListPositions[minIndex].pop(0)
                        if (len (termListPositions[minIndex]) == 0):
                            break
                        
                        window.insert(minIndex,insertNextElement)
                        if (range < minRange ):
                            minRange = range
                            print minRange
                        
                        if (minRange == 1):
                            break
                        
                    if ((not (minRange == 2147483647))):
                        score = ((1500 - minRange) * numberOfTerms ) / (lengthDict[key] + 13953998)         
                        dict[key] = score
                    
                    #okapiTfForAllTerms (float(len(termTfMap.get(key))) , 222.837521234 , float(lengthDict[str(key)]) , str(key), dict)
                    #tfIdforAllTerms (float(len(termTfMap.get(key))) , 222.837521234 , float(lengthDict[str(key)]) , str(key) , dict, float(len(termTfMap)))   
                    #okapiBm25 (float(len(termTfMap.get(key))) , 222.837521234 , float(lengthDict[str(key)]) , str(key) , dict, float(len(termTfMap)), float(queryTFDict[eachTerm1]))
    
    for key in dict:
        if combinedDict.has_key(key):
            combinedDict[key] = dict.get(key) + combinedDict.get(key)
        
    writeQueryModelsToFile(combinedDict, queryNo,"proximityMyIndexer.txt",100)
    print queryNo + " Finished"

def main():
    queryFromFile()
    #findLengthOfAllDocumentFromElasticSearch()
        
main()
