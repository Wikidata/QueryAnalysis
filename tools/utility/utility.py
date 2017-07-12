# -*- coding: utf-8 -*-

def listToString(list):
    returnString = ""
    for entry in list:
        returnString += entry + ","
    return returnString[:-1]

def addMissingSlash(directoryString):
    if not directoryString.endswith("/"):
        return directoryString + "/"
    return directoryString

def addMissingDoubleCross(text):
    if not text.startswith("#"):
        return "#" + text
    return text