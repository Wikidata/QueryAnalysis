#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import re
import sys

from postprocess import processdata
from utility import utility
from rdflib.plugins import sparql
from BeautifulSoup import Comment

parser = argparse.ArgumentParser(description="This script generates a copy of the rawLogData and makes the queries anonymous" +
    "by removing all comments and normalizing all strings. Invalid queries are left out.")
parser.add_argument("--ignoreLock", "-i", help="Ignore locked file and execute anyways", action="store_true")
parser.add_argument("--monthsFolder", "-m", default="/a/akrausetud/months/", type=str, 
                    help="The folder in which the months directories are residing.")
parser.add_argument("month", type=str, help="The month for which the rawLogData should be made anonymous.")

if (len(sys.argv[1:]) == 0):
    parser.print_help()
    parser.exit()

args = parser.parse_args()

if os.path.isfile(utility.addMissingSlash(args.monthsFolder) + utility.addMissingSlash(args.month) + "locked") and not args.ignoreLock:
    print "ERROR: The month " + args.month + " is being edited at the moment. Use -i if you want to force the execution of this script."
    sys.exit()

pnCharsBase = u"[A-Z]|[a-z]|[\u00C0-\u00D6]|[\u00D8-\u00F6]|[\u00F8-\u02FF]|[\u0370-\u037D]|[\u037F-\u1FFF]|[\u200C-\u200D]|[\u2070-\u218F]|[\u2C00-\u2FEF]|[\u3001-\uD7FF]|[\uF900-\uFDCF]|[\uFDF0-\uFFFD]|[\U00010000-\U000EFFFF]"

pnCharsU = pnCharsBase + "|[_]"

pnChars = pnCharsU + u"|[-]|[0-9]|\u00B7|[\u0300-\u036F]|[\u203F-\u2040]"

pnPrefix = "(" + pnCharsBase + ")" + "((" + pnChars + "|[.])*" + "(" + pnChars + ")" + ")?"

pnameNs = "(" + pnPrefix + ")" + "?:"

pnLocalEsc = "\\\\([_]|[~]|[.]|[-]|[!]|[$]|[&]|[']|[(]|[)]|[*]|[+]|[,]|[;]|[=]|[/]|[?]|[#]|[@]|[%])"

hex = "[0-9]|[A-F]|[a-f]"

percent = "%" + "(" + hex + ")" + "(" + hex + ")"

plx = "(" + percent + ")" + "|" + "(" + pnLocalEsc + ")"

pnLocal = "(" + pnCharsU + "|[:]|[0-9]|" + plx + ")((" + pnChars + "|[.]|[:]|" + plx + ")*(" + pnChars + "|[:]|" + plx + "))?"

pnameLn = pnameNs + "[ ]+" + pnLocal

prefixedName = "(" + pnameLn + ")|(" + pnameNs + ")"

iriref = u'<([^<>\\\\"{}|^`\u0000-\u0020])*>'

iriPattern = re.compile("(" + iriref + ")|(" + prefixedName + ")")

irirefPattern = re.compile(iriref)

prefixPattern = re.compile("[Pp][Rr][Ee][Ff][Ii][Xx][ ]+" + pnameNs + "[ ]*" + iriref)

class anonymizeHandler():
    
    declaredPrefixes = set()
    
    def setup(self):
        with open("../parserSettings/standardPrefixes.tsv") as s:
            sReader = csv.DictReader(s, delimiter="\t")
            for entry in sReader:
                self.declaredPrefixes.add(entry["prefix"])
    
    def isToIgnore(self, i, toIgnore):
        for entry in toIgnore:
            if entry[0] <= i <= entry[1]:
                return True
        return False
    
    def handle(self, sparqlQuery, processed):        
        if sparqlQuery != None:
            
            toIgnore = list()
            
            for m in prefixPattern.finditer(sparqlQuery):
                self.declaredPrefixes.add(m.group(1))
            
            for m in iriPattern.finditer(sparqlQuery):                
                for prefix in self.declaredPrefixes:
                    if m.group().startswith(prefix):
                        if m.group().endswith("#"):
                            toIgnore.append((m.start(0), m.end(0)))
                            break
                
                if irirefPattern.match(m.group()):
                    toIgnore.append((m.start(0), m.end(0)))
        
            commentState = False
            stringState = False
            
            lastStart = 0
            
            result = ""
            
            strings = dict()
            
            for i, letter in enumerate(sparqlQuery):
                if commentState:
                    if letter == "\n":
                        lastStart = i + 1
                        commentState = False
                        continue
                if stringState:
                    if letter == "'" or letter == '"':
                        string = sparqlQuery[lastStart + 1:i - 1]
                        if string not in strings:
                            strings[string] = str(len(strings))
                        result += "string" + strings[string]
                        lastStart = i
                        stringState = False
                        continue
                if letter == "#":
                    if not self.isToIgnore(i, toIgnore):
                        result += sparqlQuery[lastStart:i]
                        lastStart = i
                        commentState = True
                        continue
                if letter == "'" or letter == '"':
                    if not self.isToIgnore(i, toIgnore):
                        result += sparqlQuery[lastStart:i + 1]
                        lastStart = i
                        stringState = True
                        continue
            result += sparqlQuery[lastStart:]
            
            print sparqlQuery
            print result
                
                    
            
            #TODO: Remove and Replace

handler = anonymizeHandler()

handler.setup()

processdata.processMonth(handler, args.month, args.monthsFolder)