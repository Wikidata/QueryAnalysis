# -*- coding: utf-8 -*-

import re

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

class filter:
    
    parameters = dict()
    
    def setup(self, filterParameter):
        filters = filterParameter.split(",")
        
        for element in filters:
            arguments = element.split("=")
            self.parameters["#" + arguments[0]] = re.compile(arguments[1])
    
    def checkLine(self, processed):
        for key, value in self.parameters.iteritems():
            match = re.match(value, processed[key])
            if match != None:
                return True
            else:
                return False