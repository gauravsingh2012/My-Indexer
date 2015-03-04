import os
from os.path import join
import re 
from stemming.porter2 import stem
    
def generateListOfStopWords():
    listOfStopWords = []
    stopWordsPath = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/stoplist.txt"
    queryFile = open(stopWordsPath, "r")
    for line in queryFile:
        listOfStopWords.append(line.strip())
    return listOfStopWords


def generateTermMaps():
    
    path = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/ap89_collection/"
    listOfFiles = os.listdir(path);
    documentIds = []
    j = 0
    termIdMap = {}
    docIdMap = {}
    invertedListOfTerms = {}
    getCountOfDocumentInMemory = 1
    getInvertedCount = 1
    docIdNumber = 1
    termIdNumber = 1
    for file in listOfFiles:
        i = 0
        f = open(join(path, file), "r").read()
        doc = re.findall('<DOC>.*?</DOC>', f, re.DOTALL)
        documentNumbers = getDocNo(f)
        documentIds = documentIds + documentNumbers
        print len(documentIds)
        corpusTextList = []
        for d in doc:
            mergedStr = ""
            corpusTextContent = getTextInfo(d)                
            if len(corpusTextContent) > 1:   
                mergedStr = (mergeTwoTextTags(corpusTextContent, mergedStr))
                if (not docIdMap.has_key(documentNumbers[i])):
                    docIdMap[documentNumbers[i]] = docIdNumber
                    docIdNumber += 1
                
                terms = re.finditer('\w+(\.?\w+)*', mergedStr)
                for termPosition,term in enumerate(terms):
                    termPositionList = []
                    lowerCaseTerms = stem(term.group().lower())                    
                    if (not termIdMap.has_key(lowerCaseTerms)):
                        termIdMap[lowerCaseTerms] = termIdNumber
                        termIdNumber += 1                
            else:
                corpusTextContentString = corpusTextContent[0]
                if (not docIdMap.has_key(documentNumbers[i])):
                    docIdMap[documentNumbers[i]] = docIdNumber
                    docIdNumber += 1
                
                terms = re.finditer('\w+(\.?\w+)*', corpusTextContentString)
                for termPosition,term in enumerate(terms):
                    termPositionList = []
                    lowerCaseTerms = stem(term.group().lower())                    
                    if (not termIdMap.has_key(lowerCaseTerms)):
                        termIdMap[lowerCaseTerms] = termIdNumber
                        termIdNumber += 1               
            i = i + 1
            
    writeDictToFile(termIdMap,"termIdMapStemmer.txt")
    writeDictToFile(docIdMap,"docIdMapStemmer.txt")    
    
    #writeToFile(invertedListOfTerms, "invertedList.txt")

def readDictToMemory(dict,filename):
    file = open(filename, 'r')
    for line in file:
        splitLine = line.split()
        dict[splitLine[0]] = int (splitLine[1]) 
    return dict

def readDocumentList():
    
    path = "C:/Users/Gaurav/Downloads/AP89_DATA/AP_DATA/ap89_collection/"
    listOfFiles = os.listdir(path);
    documentIds = []
    j = 0
    termIdMap = {}
    docIdMap = {}
    invertedListOfTerms = {}
    getCountOfDocumentInMemory = 1
    getInvertedCount = 1
    termIdMap = readDictToMemory(termIdMap,"termIdMapStemmer.txt")
    docIdMap = readDictToMemory(docIdMap,"docIdMapStemmer.txt")
    print len(termIdMap)
    print len(docIdMap)
    for file in listOfFiles:
        i = 0
        f = open(join(path, file), "r").read()
        doc = re.findall('<DOC>.*?</DOC>', f, re.DOTALL)
        documentNumbers = getDocNo(f)
        documentIds = documentIds + documentNumbers
        print len(documentIds)
        corpusTextList = []
        for d in doc:
            mergedStr = ""
            corpusTextContent = getTextInfo(d)                
            if len(corpusTextContent) > 1:   
                mergedStr = (mergeTwoTextTags(corpusTextContent, mergedStr))
                addDocumentToIndex(documentNumbers[i], mergedStr, termIdMap, docIdMap, invertedListOfTerms)
            else:
                corpusTextContentString = corpusTextContent[0]
                addDocumentToIndex(documentNumbers[i], corpusTextContentString, termIdMap, docIdMap, invertedListOfTerms)      
            i = i + 1
            
            getCountOfDocumentInMemory += 1
            if (getCountOfDocumentInMemory == 1000):
                filenameInverted = "index/invertedList" + str(getInvertedCount) + ".txt"
                catalogFilename = "index/catalog/catalog" + str(getInvertedCount) + ".txt"
                writeToPartialInvertedListFile(invertedListOfTerms,filenameInverted,catalogFilename)
                getCountOfDocumentInMemory = 0
                invertedListOfTerms = {}
                getInvertedCount += 1
    
    
    mergeFiles()
    
def writeDictToFile(map,filename):
    output = open(filename, 'a')
    for key in map:
        output.write(str(key) + " " + str(map[key]) + "\n")
    

def mergeFiles():
    path = "F:/work/Workspaces/Test/index/catalog"
    listOfFiles = os.listdir(path);
    numberOfCatalogFiles = len(listOfFiles)
    
    while (len(listOfFiles) != 1):
        file1 = listOfFiles.pop()
        file2 = listOfFiles.pop()
        partialIndexFilePath1 = 'F:/work/Workspaces/Test/index/' + file1.replace('catalog','invertedList')
        partialIndexFilePath2 = 'F:/work/Workspaces/Test/index/' + file2.replace('catalog','invertedList')
        print partialIndexFilePath1
        
        with open('F:/work/Workspaces/Test/index/catalog/' + file1) as f:
            lines1 = f.read().splitlines()
        
        with open('F:/work/Workspaces/Test/index/catalog/' + file2) as f:
            lines2 = f.read().splitlines()
        
        numberOfCatalogFiles = numberOfCatalogFiles + 1
        partialIndexFilePath3 = 'F:/work/Workspaces/Test/index/' + 'invertedList' + str(numberOfCatalogFiles) + ".txt"
        catalogFileStr = merge(lines1,lines2,partialIndexFilePath1,partialIndexFilePath2,partialIndexFilePath3,numberOfCatalogFiles)
        
        listOfFiles.append(catalogFileStr)

def merge(lines1,lines2,partialIndexFilePath1,partialIndexFilePath2,partialIndexFilePath3,numberOfCatalogFiles):
    partialIndexFile1 = open(partialIndexFilePath1, 'r')
    partialIndexFile2 = open(partialIndexFilePath2, 'r')
    partialIndexFile3 = open(partialIndexFilePath3, 'a')
    partialcatalogFile = open('F:/work/Workspaces/Test/index/catalog/catalog' + str(numberOfCatalogFiles) + '.txt', 'a')
    i = 0
    j = 0
    
    while (i < len(lines1) and j < len(lines2)):
        splitTermOffset1 = lines1[i].split()
        splitTermOffset2 = lines2[j].split()
        if (int(splitTermOffset1[0]) < int(splitTermOffset2[0])):
            splitOffset = splitTermOffset1[1].split(',')
            partialIndexFile1.seek(long(splitOffset[0]))
            startOffset = partialIndexFile3.tell()
            writeTerm = partialIndexFile1.read(long(splitOffset[1]) - 1 - long(splitOffset[0]))
            partialIndexFile3.write(writeTerm)
            endOffset = partialIndexFile3.tell()
            partialcatalogFile.write(splitTermOffset1[0] + " " + str(startOffset) + "," + str(endOffset) + "\n")
            i = i + 1
        elif (int(splitTermOffset1[0]) > int(splitTermOffset2[0])):
            splitOffset = splitTermOffset2[1].split(',')
            partialIndexFile2.seek(long(splitOffset[0]))
            startOffset = partialIndexFile3.tell()
            writeTerm = partialIndexFile2.read(long(splitOffset[1]) - 1 - long(splitOffset[0]))
            partialIndexFile3.write(writeTerm)
            endOffset = partialIndexFile3.tell()
            partialcatalogFile.write(splitTermOffset2[0] + " " + str(startOffset) + "," + str(endOffset) + "\n")
            j = j + 1
        else:    
            splitOffset1 = splitTermOffset1[1].split(',')
            splitOffset2 = splitTermOffset2[1].split(',')
            partialIndexFile1.seek(long(splitOffset1[0]))
            partialIndexFile2.seek(long(splitOffset2[0]))
            writeTerm1 = partialIndexFile1.read(long(splitOffset1[1]) - 2 - long(splitOffset1[0]))
            writeTerm2 = partialIndexFile2.read(long(splitOffset2[1]) - 1 - long(splitOffset2[0]))
            writeTerm = writeTerm1 + writeTerm2.replace(splitTermOffset1[0],'')
            startOffset = partialIndexFile3.tell()
            partialIndexFile3.write(writeTerm)
            endOffset = partialIndexFile3.tell()
            partialcatalogFile.write(splitTermOffset1[0] + " " + str(startOffset) + "," + str(endOffset) + "\n")
            i = i + 1
            j = j + 1
            
    while (i < len(lines1)):
        splitTermOffset1 = lines1[i].split()
        splitOffset = splitTermOffset1[1].split(',')
        partialIndexFile1.seek(long(splitOffset[0]))
        writeTerm = partialIndexFile1.read(long(splitOffset[1]) - 1 - long(splitOffset[0]))
        startOffset = partialIndexFile3.tell()
        partialIndexFile3.write(writeTerm)
        endOffset = partialIndexFile3.tell()
        partialcatalogFile.write(splitTermOffset1[0] + " " + str(startOffset) + "," + str(endOffset) + "\n")
        i = i + 1
            
    while (j < len(lines2)):
        splitTermOffset2 = lines2[j].split()
        splitOffset = splitTermOffset2[1].split(',')
        partialIndexFile2.seek(long(splitOffset[0]))
        writeTerm = partialIndexFile2.read(long(splitOffset[1]) - 1 - long(splitOffset[0]))
        startOffset = partialIndexFile3.tell()
        partialIndexFile3.write(writeTerm)
        endOffset = partialIndexFile3.tell()
        partialcatalogFile.write(splitTermOffset2[0] + " " + str(startOffset) + "," + str(endOffset) + "\n")
        j = j + 1
        
    return 'catalog' + str(numberOfCatalogFiles) + '.txt'
    
def addDocumentToIndex(docId, corpusContent,termIdMap, docIdMap, invertedListOfTerms):
    file = open("lengthOfDocumentsStemmer.txt",'a')
    terms = re.finditer('\w+(\.?\w+)*', corpusContent)
    termDict = {}
    lengthOfDocument = 0
    for termPosition,term in enumerate(terms):
        lengthOfDocument = termPosition
        termPositionList = []
        lowerCaseTerms = stem(term.group().lower()) 
        termId = termIdMap.get(lowerCaseTerms)
        docIdMapNumber = docIdMap.get(docId)
        termDict[lowerCaseTerms] = True
        if (invertedListOfTerms.has_key(termId)):        
            if (invertedListOfTerms[termId].has_key(docIdMapNumber)):            
                termPositionList = invertedListOfTerms[termId].get(docIdMapNumber)
                termPositionList.append(termPosition)
                invertedListOfTerms[termId][docIdMapNumber] = termPositionList 
            else:
                termPositionList.insert(0, termPosition)
                invertedListOfTerms[termId][docIdMapNumber] = termPositionList
        else:
            termPositionList.insert(0, termPosition)
            invertedListOfTerms[termId] = {}    
            invertedListOfTerms[termId][docIdMapNumber] = termPositionList
    
    file.write(docId + " " + str(len(termDict)) + "\n")
    
def writeToPartialInvertedListFile(dict, filename,catalogFilename):
    output = open(filename, 'a')
    catalog = open(catalogFilename, 'a')
    
    for key in sorted(dict):
        invertedListValueStr = ""
        for keyDocId in dict[key]:
            positionStr = ""
            for keyPosition in dict[key][keyDocId]:
                positionStr = positionStr + " " + str(keyPosition)
            invertedListValueStr = invertedListValueStr + str(keyDocId) + positionStr + ","
            
        invertedListValueStr = invertedListValueStr[:-1]  
        startOffset = output.tell();  
        output.write(str(key) + "," + invertedListValueStr + "\n")
        endOffset = output.tell();
        catalog.write(str(key) + " " + str(startOffset) + "," + str(endOffset) + "\n")
        
        
    output.close()
   
    
def mergeTwoTextTags(corpusTextContent, mergedStr):
    for str in corpusTextContent:
        mergedStr = mergedStr + str
    return mergedStr
    
def getTextInfo(d):
    listOfRemovedTags = []
    text = re.findall('<TEXT>.*?</TEXT>', d, re.DOTALL)
    for elem in text:
        removedTagString = re.sub('<.*?>', '', elem)
        listOfRemovedTags.append(removedTagString)
    return listOfRemovedTags

def getDocNo(f):
    docNo = re.findall('<DOCNO>.*</DOCNO>', f)
    listOfRemovedTags = []
    for d in docNo:
        removedTagString = re.sub('<.*?>', '', d)
        removedTagString = removedTagString.strip()
        listOfRemovedTags.append(removedTagString)
    return listOfRemovedTags


def main():
    #generateTermMaps()
    readDocumentList()
    
main()