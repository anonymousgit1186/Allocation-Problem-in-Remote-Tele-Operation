import json
import csv
import collections
from Worker import *
from BaseManager import BaseManager


class WorkersManager:

    def __init__(self):

        self.mWorkersList = {}

    @staticmethod
    def getWorkerDescriptionFromXml(xmlObject):

        emptyWorkerDescription = Worker.toArray(Worker(''))

        return BaseManager.getObjectDescriptionFromXml(xmlObject, emptyWorkerDescription)

    def getWorkerDescriptionFromCSVRow(fieldNames, csvRow):

        emptyWorkerDescription = Worker.toArray(Worker(''))

        for index in range(len(fieldNames)):

            if ('' == fieldNames[index]):
                continue

            emptyWorkerDescription[fieldNames[index]] = csvRow[index]

        return emptyWorkerDescription

    def addWorkersFromXML(self, lWorkersXMLNode):

        for worker in lWorkersXMLNode:
            workerDescription = WorkersManager.getWorkerDescriptionFromXml(worker)

            self.addWorker(workerDescription)

    def addWorkersFromCSV(self, csvFilePath):

        isFirstRow = True
        fieldNames = []

        with open(csvFilePath, 'r') as csvfile:

            csv_reader = csv.reader(csvfile, delimiter=',')

            for row in csv_reader:

                if (isFirstRow):
                    isFirstRow = False

                    fieldNames = row
                    
                    continue

                if ('' == row or len(row) == 0):
                    continue

                workerDescription = WorkersManager.getWorkerDescriptionFromCSVRow(fieldNames, row)

                workerDescription['index'] = len(self.getWorkersList())

                self.addWorker(workerDescription)

    def addWorker(self, lWorkerDescriptionArr):

        newWorkerId = lWorkerDescriptionArr['workerId']

        worker = Worker(newWorkerId)
        worker.loadFromArr(lWorkerDescriptionArr)

        self.mWorkersList[newWorkerId] = worker

    def getWorkerById(self, lId):

        if (lId not in self.mWorkersList):
            return None

        return self.mWorkersList[lId]

    def getWorkerByIndex(self, workerIndex):

        if (len(list(self.mWorkersList.keys())) <= workerIndex):
            return None
        
        return list(self.mWorkersList.items())[workerIndex][1]

    def onTick(self, currentTime=-1):

        for workerId, worker in self.mWorkersList.items():
            worker.onTick(currentTime)

    def getFirstFreeWorker(self, currentTime):

        for workerId, worker in self.mWorkersList.items():

            if (not worker.isAvailable(currentTime)):
                continue

            return worker

        return None

    def getWorkerByName(self, lWorkerName):

        raise NotImplementedError

    def getWorkersList(self):

        return self.mWorkersList

    def getWorkersListLength(self):

        return len(self.mWorkersList)

    def getWorkersIdsList(self):

        return self.mWorkersList.keys()

    def updateAllWorkersOnlineStatus(self, currentTime):
        
        for workerId, worker in self.mWorkersList.items():

            worker.updateOnlineStatus(currentTime, 'force_status_update')

    def dot(self, dot, currentTime):
        dot.attr(rank='same', label='', rankdir='LR', ordering="out")
        sorted_dict = collections.OrderedDict(sorted(self.mWorkersList.items()))
        prev_worker = None
        for workerID, worker in sorted_dict.items():
            worker.dot(dot, currentTime)
            if prev_worker:
                dot.edge('W ' + str(prev_worker), 'W ' + str(workerID), color='transparent', ordering="out")
            prev_worker = workerID
