import collections
import csv
import json
import random

from Job import *
from BaseManager import BaseManager


class JobsManager:

    def __init__(self, f):

        self.mJobsList = {}

        self.mCompletedJobsList = {}
        self.f = f

    @staticmethod
    def filterJobs(jobsList, maxArrivalTime, unassignedOnly=True):

        output = {}

        for jobId, job in jobsList.items():

            if (unassignedOnly and job.isAssigned()):
                continue

            if (job.getArrivalTime() > maxArrivalTime):
                continue

            output[jobId] = job

        return output

    @staticmethod
    def futureJobs(jobsList, maxArrivalTime):

        output = {}

        for jobId, job in jobsList.items():

            if (job.getArrivalTime() > maxArrivalTime):
                output[jobId] = job

        return output

    @staticmethod
    def getJobDescriptionFromXml(xmlObject):

        emptyJobDescription = Job.toArray(Job('',-1))

        return BaseManager.getObjectDescriptionFromXml(xmlObject, emptyJobDescription)

    def getJobDescriptionFromCSVRow(fieldNames, csvRow, f):

        emptyJobDescription = Job.toArray(Job('', -1, f))

        for index in range(len(fieldNames)):

            if ('' == fieldNames[index]):
                continue

            emptyJobDescription[fieldNames[index].replace(' ', '')] = csvRow[index]

        return emptyJobDescription

    def addJobsFromXML(self, lJobsXMLNode):

        for job in lJobsXMLNode:
            jobDescription = JobsManager.getJobDescriptionFromXml(job)
            jobDescription['jobId'] = job.get('id')

            self.addJob(jobDescription)

    def addJobsFromCSV(self, csvFilePath):

        isFirstRow = True
        fieldNames = []

        with open(csvFilePath, 'r') as csvfile:

            csv_reader = csv.reader(csvfile, delimiter=',')

            for row in csv_reader:

                if (isFirstRow):
                    isFirstRow = False

                    fieldNames = row

                    continue

                if ('' == row or len(row) == 0 or '' == row[0]):
                    continue

                jobDescription = JobsManager.getJobDescriptionFromCSVRow(fieldNames, row, self.f)

                self.addJob(jobDescription)

    def addJob(self, lJobDescriptionArr):

        if ('' != lJobDescriptionArr['jobId']):
            newJobId = lJobDescriptionArr['jobId']
        else:
            newJobId = lJobDescriptionArr['job_id']

        job = Job(newJobId, len(list(self.mJobsList.keys())), self.f)
        job.loadFromArr(lJobDescriptionArr)

        self.mJobsList[newJobId] = job

    def getJobById(self, lId, onlyNotCompletedJob=False):

        if (lId not in self.mJobsList):

            if (onlyNotCompletedJob or lId not in self.mCompletedJobsList):
                return None

            return self.mCompletedJobsList[lId]

        return self.mJobsList[lId]

    def getJobByIndex(self, jobIndex):

        for jobId, job in self.mJobsList.items():
            if job.getIndex() == jobIndex:
                return job
        return None

    def getJobByName(self, jobName):

        raise NotImplementedError

    def removeJob(self, jobId, moveToCompleted=True):

        try:

            if (moveToCompleted):
                job = self.mJobsList[jobId]

                jobClone = Job(jobId, job.getIndex(), self.f)
                jobClone.loadFromArr(Job.toArray(job))

                self.mCompletedJobsList[jobId] = jobClone

            del self.mJobsList[jobId]

            return True
        except Exception as e:
            print('Failed to remove job ' + str(jobId), file=self.f)
            raise e
            return False

    def onTick(self, statistics, C, currentTime=-1):

        jobsIdsToBeRejected = []

        for jobId, job in self.mJobsList.items():
            job.onTick(currentTime)
            if job.getWaitingTime() >= C:
                jobsIdsToBeRejected.append(jobId)
                statistics.writeRejectedJob(currentTime, job, 'notAssignedYet', 'notAssignedYet', 'long_waiting_time')

        for jobId in jobsIdsToBeRejected:
            self.removeJob(jobId, False)

    def getFirstWaitingJob(self):

        for jobId, job in self.mJobsList.items():

            if (job.isAssigned()):
                continue

            return job

        return None

    def getJobIndexByJobId(self, jobId):

        job = self.getJobById(jobId)

        if (None == jobId):
            return -1

        return job.getIndex()

    def getJobTypeIndexByJobId(self, searchJobId):
        for jobId, job in self.mJobsList.items():
            if (searchJobId == jobId):
                scenario_type = job.getScenarioType()
                scenario_type_index = int(scenario_type[-1])
                return scenario_type_index
        return -1

    def getRandomWaitingJob(self):

        selectedJob = None

        for jobId, job in self.mJobsList.items():

            if (job.isAssigned()):
                continue

            if (random.randint(1, 2) == 1):
                return job
            else:
                selectedJob = job

        return selectedJob

    def getCompletedJobsList(self):

        return self.mCompletedJobsList

    def getJobsList(self):

        return self.mJobsList

    def getJobsListLength(self):

        return len(self.mJobsList)

    def getWaitingJobsCount(self):

        counter = 0

        for jobId, job in self.mJobsList.items():

            if (job.isAssigned()):
                continue

            counter += 1

        return counter

    def dot(self, dot, tick):
        dot.attr(rank='same', label='', rankdir='LR')
        sorted_dict = collections.OrderedDict(sorted(self.mJobsList.items()))
        prev_job = None
        for jobId, job in sorted_dict.items():
            if job.getArrivalTime() > tick:
                continue
            job.dot(dot)
            if prev_job:
                dot.edge(str(prev_job), str(jobId), color='transparent', dir='LR')
            prev_job = jobId
