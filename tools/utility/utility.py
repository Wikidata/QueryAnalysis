# -*- coding: utf-8 -*-

import re

# This list contains all fields that should not be split because they could contain commas
notToSplit = ["user_agent", "ToolName"]

def listToString(list):
    returnString = ""
    for entry in list:
        returnString += entry + ","
    return returnString[:-1]

def addMissingSlash(directoryString):
    if not directoryString.endswith("/"):
        return directoryString + "/"
    return directoryString

def argMetric(metric):
    if metric.startswith("#"):
        return metric[1:]
    else:
        return metric

def fetchEntries(processed, metric, nosplitting = False):
    metric = argMetric(metric)
    if metric == "monthly_hour":
        try:
            hour = int(processed["hour"])
        except ValueError:
            print processed["hour"] + " could not be parsed as integer"
            return []
        if hour not in xrange(0,24):
            print str(hour) + " is not in 0-23"
            return []
        try:
            day = int(processed["day"])
        except:
            print processed["day"] + " could not be parsed as integer"
            return []

        return [hour + 24 * (day - 1)]
    else:
        data = processed[metric]
        if metric in notToSplit:
            return [data]
        else:
            field_array = splitEntry(data)
            if nosplitting:
                field_array = sorted(field_array)
                return [listToString(field_array)]
            else:
                return field_array

def splitEntry(entry):
    field_array = entry.split(",")
    field_array = map(lambda it: it.strip(), field_array)
    field_array = [x for x in field_array if  x]
    return field_array

class filter:

    parameters = dict()

    def setup(self, filterParameter):
        self.parameters["Valid"] = re.compile("^VALID$")

        filters = filterParameter.split(",")

        if filters == ['']:
            return

        for element in filters:
            arguments = element.split("=")
            self.parameters[arguments[0]] = re.compile(arguments[1])

    def checkLine(self, processed):
        for key, value in self.parameters.iteritems():
            match = re.match(value, str(processed[key]))
            if match == None:
                return False
        return True
