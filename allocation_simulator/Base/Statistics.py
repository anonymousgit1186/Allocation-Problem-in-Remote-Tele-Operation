
import numpy as np

import os
import sys
import time

sys.path.insert(1, os.path.join(os.path.dirname(__file__) + '/../', 'Algorithm'))
from Algorithm.AlgorithmHelper import AlgorithmHelper

class Statistics:


    def __init__(self, executionUniqueIndex):

        pathToDataDir = os.path.join(os.path.dirname(__file__) + '/../', 'data')
        pathToStatsDir = os.path.join(os.path.dirname(__file__) + '/../', 'stats')

        self.executionUniqueIndex = executionUniqueIndex

        self.timeQualityStatsLogFilePath  = pathToDataDir + "/time_quality.csv"

        self.statsDirectory = pathToStatsDir + "/" + executionUniqueIndex + '/'

        self.allocationValuesDirectory = self.statsDirectory + 'allocation_values/'


        self.workersOfflineStatusesFilePath = self.statsDirectory + "workers_offline_statuses_summary.csv"

        self.pauseEventsRecordsFilePath = self.statsDirectory + "pause_events_summary.csv"

        self.experimentSummaryFilePath = self.statsDirectory + "experiment_summary.csv"

        self.jobsCompletionLogFilePath = self.statsDirectory + "jobs_completion_log.csv"


        self.algorithmDebugFilePath = self.statsDirectory + "algorithm_debug.csv"

        self.assignmentsLogFilePath = self.statsDirectory + "assignments_log.csv"

        self.systemPauseStatusFilePath = self.statsDirectory + "system_pause_status.csv"

        self.workerOnlineStatusFilePath = self.statsDirectory + "worker_online_status.csv"

        self.waitingJobsIdsFilePath = self.statsDirectory + "waiting_jobs_ids.csv"

        self.rejectedJobsFilePath = self.statsDirectory + "rejected_jobs.csv"

        self.assignmentsListAtiFilePath = self.statsDirectory + "assignmentsListAti.csv"

        self.resetFiles()

    def resetFiles(self):

        if not os.path.exists(self.statsDirectory):
            os.makedirs(self.statsDirectory)

        if not os.path.exists(self.allocationValuesDirectory):
            os.makedirs(self.allocationValuesDirectory)


        workersOfflineStatusesFile = open(self.workersOfflineStatusesFilePath, 'w+')
        workersOfflineStatusesFile.close()

        pauseEventsRecordsFile = open(self.pauseEventsRecordsFilePath, 'w+')
        pauseEventsRecordsFile.close()

        experimentSummaryFile = open(self.experimentSummaryFilePath, 'w+')
        experimentSummaryFile.close()

        jobsCompletionLogFile = open(self.jobsCompletionLogFilePath, 'w+')
        jobsCompletionLogFile.close()

        algorithmDebugFile = open(self.algorithmDebugFilePath, 'w+')
        algorithmDebugFile.close()

        assignmentsLogFile = open(self.assignmentsLogFilePath, 'w+')
        assignmentsLogFile.close()

        systemPauseStatusFile = open(self.systemPauseStatusFilePath, 'w+')
        systemPauseStatusFile.close()

        workerOnlineStatusFile = open(self.workerOnlineStatusFilePath, 'w+')
        workerOnlineStatusFile.close()

        waitingJobsStatusFile = open(self.waitingJobsIdsFilePath, 'w+')
        waitingJobsStatusFile.close()

        rejectedJobsFilePathFile = open(self.rejectedJobsFilePath, 'w+')
        rejectedJobsFilePathFile.close()

        assignmentsListAtiFile = open(self.assignmentsListAtiFilePath, 'w+')
        assignmentsListAtiFile.close()



    def writeTimeQualityStats(self, statsArr, writeMode = 'w+'):
        pass

    def writeJobCompletionByOperatorFileTitle(self, writeMode = 'w+'):

        jobsCompletionLogFile = open(self.jobsCompletionLogFilePath, writeMode)

        jobsCompletionLogFile.write('RecordTimeStamp,JobId,WorkerId,JobArrivalTick,JobAssignmentTick,JobFetchTick,JobStartByOperatorTime,JobActiveTime,JobRemainingTime,CleanWorkingTime,JobEndByOperatorTime,WorkerFreeAgainTick')
        jobsCompletionLogFile.write('\n')

        jobsCompletionLogFile.close()

    def logJobCompletionByOperator(self, jobId, workerId, jobArrivalTime, jobAssignmentTime, jobFetchTime, jobStartTime, jobActiveTime, jobRemainedTime, currentTick, workerFreeAgainTick, writeMode = 'a+'):

        jobsCompletionLogFile = open(self.jobsCompletionLogFilePath, writeMode)

        jobsCompletionLogFile.write(str(int(time.time())) + ',')

        jobsCompletionLogFile.write(str(jobId) + ',')
        jobsCompletionLogFile.write(str(workerId) + ',')
        jobsCompletionLogFile.write(str(jobArrivalTime) + ',')
        jobsCompletionLogFile.write(str(jobAssignmentTime) + ',')
        jobsCompletionLogFile.write(str(jobFetchTime) + ',')
        jobsCompletionLogFile.write(str(jobStartTime) + ',')
        jobsCompletionLogFile.write(str(jobActiveTime) + ',')
        jobsCompletionLogFile.write(str(jobRemainedTime) + ',')

        try:
            if (int(jobStartTime) > 0):
                jobsCompletionLogFile.write(str(int(currentTick) - int(jobStartTime)) + ',')
            else:
                jobsCompletionLogFile.write('-1,')
        except:
            
            jobsCompletionLogFile.write('-1,')
            
        jobsCompletionLogFile.write(str(currentTick) + ',')
        jobsCompletionLogFile.write(str(workerFreeAgainTick) + ',')

        jobsCompletionLogFile.write('\n')

        jobsCompletionLogFile.close()

    def writePauseEventsRecords(self, statsArr, shouldWriteTitle = False, writeMode = 'w+'):

        pauseEventsRecordsFile = open(self.pauseEventsRecordsFilePath, writeMode)

        if (shouldWriteTitle):
            pauseEventsRecordsFile.write('SystemPauseEventTick')
            pauseEventsRecordsFile.write('\n')


        if (len(statsArr) == 0):
            return


        for systemTick in statsArr:
            pauseEventsRecordsFile.write(str(systemTick) + ',')
            pauseEventsRecordsFile.write('\n')


        pauseEventsRecordsFile.close()


    def writeAllocationValuesLog(self, statsDict, currentSystemTick, writeMode = 'w+'):

        allocationValuesFile = open(self.allocationValuesDirectory + 'tick_' + str(currentSystemTick) + '.csv', writeMode)

        # Write title row
        allocationValuesFile.write(',')

        for workerNode in statsDict.keys():
            for jobNode in statsDict[workerNode].keys():
                allocationValuesFile.write(str(jobNode) + ',')
            
            allocationValuesFile.write('\n')

            break


        for workerNode in statsDict.keys():
            
            allocationValuesFile.write(str(workerNode) + ',')

            for jobNode in statsDict[workerNode].keys():

                allocationValuesFile.write(str(statsDict[workerNode][jobNode]) + ',')
                
            allocationValuesFile.write('\n')


        allocationValuesFile.close()


    def writeSystemPauseStatusTitle(self, writeMode = 'w+'):

        systemPauseStatusFile = open(self.systemPauseStatusFilePath, writeMode)

        systemPauseStatusFile.write('RecordTimeStamp,SystemIsPauseNow,LastOfflineWokerId,CurrentSystemTick')
        systemPauseStatusFile.write('\n')

        systemPauseStatusFile.close()

    def writeSystemPauseStatus(self, systemIsPauseNow, lastOfflineWokerId, systemTick, writeMode = 'a+'):

        systemPauseStatusFile = open(self.systemPauseStatusFilePath, writeMode)

        systemPauseStatusFile.write(str(int(time.time())) + ',')

        systemPauseStatusFile.write(('1' if systemIsPauseNow else '0') + ',')
        systemPauseStatusFile.write(str(lastOfflineWokerId) + ',')
        systemPauseStatusFile.write(str(systemTick) + ',')
        systemPauseStatusFile.write('\n')

        systemPauseStatusFile.close()


    def writeWorkerOnlineStatusTitle(self, writeMode = 'w+'):

        workerOnlineStatusFile = open(self.workerOnlineStatusFilePath, writeMode)

        workerOnlineStatusFile.write('RecordTimeStamp,WorkerId,isWorkerCausedPause,IsWorkerBusyNow,WorkerAssignedJobId,WorkerOfflineTime,LastOnlineStatusUpdateTick,LastOnlineIndicatorsCount,LastOnlineStatusJobId,CurrentSystemTick')
        workerOnlineStatusFile.write('\n')

        workerOnlineStatusFile.close()

    def writeWorkerOnlineStatus(self, workerId, isWorkerCausedPause, isWorkerBusyNow, busyOnJobId, workerOfflineTime, lastOnlineStatusUpdateTick, lastOnlineIndicatorsCount, lastOnlineStatusJobId, systemTick, writeMode = 'a+'):

        workerOnlineStatusFile = open(self.workerOnlineStatusFilePath, writeMode)

        workerOnlineStatusFile.write(str(int(time.time())) + ',')

        workerOnlineStatusFile.write(str(workerId) + ',')
        workerOnlineStatusFile.write(('1' if isWorkerCausedPause else '0') + ',')
        workerOnlineStatusFile.write(('1' if isWorkerBusyNow else '0') + ',')
        workerOnlineStatusFile.write(str(busyOnJobId) + ',')
        workerOnlineStatusFile.write(str(workerOfflineTime) + ',')
        workerOnlineStatusFile.write(str(lastOnlineStatusUpdateTick) + ',')
        workerOnlineStatusFile.write(str(lastOnlineIndicatorsCount) + ',')
        workerOnlineStatusFile.write(str(lastOnlineStatusJobId) + ',')
        workerOnlineStatusFile.write(str(systemTick) + ',')
        workerOnlineStatusFile.write('\n')

        workerOnlineStatusFile.close()

    def writeWaitingJobsIdsTitle(self, writeMode = 'w+'):

        waitingJobsIdsFile = open(self.waitingJobsIdsFilePath, writeMode)

        waitingJobsIdsFile.write('RecordTimeStamp,CurrentSystemTick,JobsIdsList')
        
        waitingJobsIdsFile.write('\n')

        waitingJobsIdsFile.close()

    def writeWaitingJobsIds(self, jobsIdsArr, systemTick, writeMode = 'a+'):

        waitingJobsIdsFile = open(self.waitingJobsIdsFilePath, writeMode)

        waitingJobsIdsFile.write(str(int(time.time())) + ',')
        waitingJobsIdsFile.write(str(systemTick) + ',')

        for jobId in jobsIdsArr:

            waitingJobsIdsFile.write(str(jobId) + ',')

        waitingJobsIdsFile.write('\n')

        waitingJobsIdsFile.close()

    def writeRejectedJobsTitle(self, writeMode = 'w+'):

        rejectedJobsFile = open(self.rejectedJobsFilePath, writeMode)

        rejectedJobsFile.write('RecordTimeStamp,CurrentSystemTick,JobsId,WorkerId,ArrivalTime,AssignTime,Reason')
        
        rejectedJobsFile.write('\n')

        rejectedJobsFile.close()

    def writeRejectedJob(self, systemTick, removedJob, workerId, assignedTime, reason, writeMode = 'a+'):

        rejectedJobsFile = open(self.rejectedJobsFilePath, writeMode)

        rejectedJobsFile.write(str(int(time.time())) + ',')
        rejectedJobsFile.write(str(systemTick) + ',')

        rejectedJobsFile.write(str(removedJob.getId()) + ',')
        rejectedJobsFile.write(str(workerId) + ',')
        rejectedJobsFile.write(str(removedJob.getArrivalTime()) + ',')
        rejectedJobsFile.write(str(assignedTime) + ',')
        rejectedJobsFile.write(str(reason) + ',')

        rejectedJobsFile.write('\n')

        rejectedJobsFile.close()

    def writeWorkersOfflineStatuses(self, statsArr, shouldWriteTitle = False, writeMode = 'w+'):

        workersOfflineStatusesFile = open(self.workersOfflineStatusesFilePath, writeMode)

        if (shouldWriteTitle):
            workersOfflineStatusesFile.write('WorkerId,WorkerOfflineTick,')
            workersOfflineStatusesFile.write('\n')


        if (len(statsArr) == 0):
            return


        for workerId in statsArr:

            workerOfflineTimeStamps = statsArr[workerId]

            for systemTick in workerOfflineTimeStamps:
           
                workersOfflineStatusesFile.write(str(workerId) + ',')
                workersOfflineStatusesFile.write(str(systemTick) + ',')
                workersOfflineStatusesFile.write('\n')


        workersOfflineStatusesFile.close()

    def writeAssignmentsListAti(self, LP_AssignmentsList, workersManager, writeMode='w+'):

        assignmentsListAtiFile = open(self.assignmentsListAtiFilePath, writeMode)

        assignmentsListAtiFile.write(str('tickNumber') + ',')

        for i in range(len(LP_AssignmentsList[0])):
            worker = workersManager.getWorkerByIndex(i)
            assignmentsListAtiFile.write(str(worker.getId()) + ',')

        assignmentsListAtiFile.write('\n')

        for t in range(len(LP_AssignmentsList)):

            assignmentsListAtiFile.write(str(t) + ',')

            for jobsList in LP_AssignmentsList[t]:
                tempStr = ''
                for x in jobsList:
                    tempStr+='|'
                    for y in x:
                        tempStr += str(y) + '_'
                tempStr+='|'
                assignmentsListAtiFile.write(str(tempStr[:-1]) + ',')

            assignmentsListAtiFile.write('\n')

        assignmentsListAtiFile.close()

    def writeAssignmentLog(self, statsArr, shouldWriteTitle = False, writeMode = 'a+'):

        assignmentsLogFile = open(self.assignmentsLogFilePath, writeMode)

        if (shouldWriteTitle):
            assignmentsLogFile.write('RecordTimeStamp,SystemTick,AssignmentTick,JobId,WorkerId,JobArrivalTime,JobTime,FreeWorkersCount')
            assignmentsLogFile.write('\n')


        if (len(statsArr) == 0):
            return


        assignmentsLogFile.write(str(int(time.time())) + ',')

        if ('currentTick' in statsArr):
            assignmentsLogFile.write(str(statsArr['currentTick']) + ',')
            assignmentsLogFile.write(str(int(statsArr['currentTick']) + 1) + ',')

        if ('jobId' in statsArr):
            assignmentsLogFile.write(str(statsArr['jobId']) + ',')

        if ('workerId' in statsArr):
            assignmentsLogFile.write(str(statsArr['workerId']) + ',')

        if ('jobArrivalTime' in statsArr):
            assignmentsLogFile.write(str(statsArr['jobArrivalTime']) + ',')

        if ('jobTime' in statsArr):
            assignmentsLogFile.write(str(statsArr['jobTime']) + ',')

        if ('freeWorkersCount' in statsArr):
            assignmentsLogFile.write(str(statsArr['freeWorkersCount']) + ',')


        assignmentsLogFile.write('\n')


        assignmentsLogFile.close()

    def writeAlgorithmDebugRow(self, statsArr, shouldWriteTitle = False, writeMode = 'a+'):

        algorithmDebugFile = open(self.algorithmDebugFilePath, writeMode)

        if (shouldWriteTitle):
            algorithmDebugFile.write('RecordTimeStamp,')

            title = ''
            title += 'job ID,'
            title += 'worker ID,'
            title += 'value func type,'
            title += 'time predict,'
            title += 'real time,'
            title += 'norm (alpha0 * time prediction),'
            title += 'norm (alpha0 * real time),'
            title += 'quality predict,'
            title += 'real quality,'
            title += 'norm (alpha1 * quality prediction),'
            title += 'norm (alpha1 * quality),'
            title += 'urgency,'
            title += 'norm (alpha2 * urgency),'
            title += 'alpha4 * exponent waiting time,'
            title += 'norm(alpha5 * waiting time),'
            title += 'predict value func,'
            title += 'eval func,'
            title += 'arrival time,'
            title += 'assignment time,'
            title += 'waiting time,'
            title += 'active time,'
            title += 'completed time,'
            title += 'total time,'
            title += 'h_time,'
            title += 'available workers,'

            algorithmDebugFile.write(title)

            algorithmDebugFile.write('\n')
            

        if (len(statsArr.keys()) == 0):
            return


        algorithmDebugFile.write(str(int(time.time())) + ',')


        if ('job ID' in statsArr):
            algorithmDebugFile.write(str(statsArr['job ID']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('worker ID' in statsArr):
            algorithmDebugFile.write(str(statsArr['worker ID']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('value func type' in statsArr):
            algorithmDebugFile.write(str(statsArr['value func type']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('time predict' in statsArr):
            algorithmDebugFile.write(str(statsArr['time predict']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('real time' in statsArr):
            algorithmDebugFile.write(str(statsArr['real time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('norm (alpha0 * time prediction)' in statsArr):
            algorithmDebugFile.write(str(statsArr['norm (alpha0 * time prediction)']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('norm (alpha0 * real time)' in statsArr):
            algorithmDebugFile.write(str(statsArr['norm (alpha0 * real time)']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('quality predict' in statsArr):
            algorithmDebugFile.write(str(statsArr['quality predict']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('real quality' in statsArr):
            algorithmDebugFile.write(str(statsArr['real quality']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('norm (alpha1 * quality prediction)' in statsArr):
            algorithmDebugFile.write(str(statsArr['norm (alpha1 * quality prediction)']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('norm (alpha1 * quality)' in statsArr):
            algorithmDebugFile.write(str(statsArr['norm (alpha1 * quality)']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('urgency' in statsArr):
            algorithmDebugFile.write(str(statsArr['urgency']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('norm (alpha2 * urgency)' in statsArr):
            algorithmDebugFile.write(str(statsArr['norm (alpha2 * urgency)']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('alpha4 * exponent waiting time' in statsArr):
            algorithmDebugFile.write(str(statsArr['alpha4 * exponent waiting time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('norm(alpha5 * waiting time)' in statsArr):
            algorithmDebugFile.write(str(statsArr['norm(alpha5 * waiting time)']) + ',')
        else:
            algorithmDebugFile.write('-1,')

        if ('predict value func' in statsArr):
            algorithmDebugFile.write(str(statsArr['predict value func']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('eval func' in statsArr):
            algorithmDebugFile.write(str(statsArr['eval func']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('arrival time' in statsArr):
            algorithmDebugFile.write(str(statsArr['arrival time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('assignment time' in statsArr):
            algorithmDebugFile.write(str(statsArr['assignment time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
                
        if ('waiting time' in statsArr):
            algorithmDebugFile.write(str(statsArr['waiting time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('active time' in statsArr):
            algorithmDebugFile.write(str(statsArr['active time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('completed time' in statsArr):
            algorithmDebugFile.write(str(statsArr['completed time']) + ',')
        else:
            algorithmDebugFile.write('-1,')

        if ('total time' in statsArr):
            algorithmDebugFile.write(str(statsArr['total time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('h_time' in statsArr):
            algorithmDebugFile.write(str(statsArr['h_time']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            
        if ('available workers' in statsArr):
            algorithmDebugFile.write(str(statsArr['available workers']) + ',')
        else:
            algorithmDebugFile.write('-1,')
            

        algorithmDebugFile.write('\n')

        algorithmDebugFile.close()


    def writeExperimentSummaryStats(self, statsArr, shouldWriteTitle = True, writeMode = 'w+'):

        if (len(statsArr.keys()) == 0):
            return

        experimentSummaryFile = open(self.experimentSummaryFilePath, writeMode)

        if (shouldWriteTitle):

            experimentSummaryFile.write('RecordTimeStamp,')

            for paramName in statsArr:
                experimentSummaryFile.write(paramName + ',')

            experimentSummaryFile.write('\n')


        if (len(statsArr.keys()) == 0):
            return

        
        experimentSummaryFile.write(str(int(time.time())) + ',')

        for paramName in statsArr:
           
            experimentSummaryFile.write(str(statsArr[paramName]).replace(',', ';') + ',')

        experimentSummaryFile.write('\n')

        experimentSummaryFile.close()


    def combineAlgorithmDebugStatsArr(self, inputStatsArr,C,f, writeMode = 'a+'):
        print("F IS ", f, file=f)
        outputStatsArr = {}

        job = inputStatsArr['job']
        worker = inputStatsArr['worker']
        scheduler = inputStatsArr['scheduler']
        if 'alphas' in inputStatsArr:
            alphas = inputStatsArr['alphas']
        else:
            alphas = [0.7, 0.4, 0.4, 0, 0.2, 0.6]
        if ('epsilon_precent' in inputStatsArr):
            epsilon_precent = inputStatsArr['epsilon_precent']
        else:
            epsilon_precent = 0.0
        if 'type_val_func' in inputStatsArr:
            type_val_func = inputStatsArr['type_val_func']
        else:
            type_val_func = ''
        if 'isWT' in inputStatsArr:
            isWT = inputStatsArr['isWT']
        else:
            isWT = None

        if ('currentTime' in inputStatsArr):
            currentTime = inputStatsArr['currentTime']
        else:
            currentTime = -1
        # sampled_times = inputStatsArr['sampled_times']

        outputStatsArr = {}
        # assignment time:
        outputStatsArr['job ID'] = job.getId()
        outputStatsArr['worker ID'] = worker.getId()
        outputStatsArr['value func type'] = str(type_val_func)
        outputStatsArr['urgency'] = job.getUrgency()
        outputStatsArr['norm (alpha2 * urgency)'] = AlgorithmHelper.normalizeUrgency(job.getUrgency()) * alphas[2]
        outputStatsArr['alpha4 * exponent waiting time'] = pow((1 + AlgorithmHelper.ALGORITHM_DELTA), job.getWaitingTime()) * alphas[4]
        outputStatsArr['norm(alpha5 * waiting time)'] = AlgorithmHelper.normalizeWaitingTime(job.getWaitingTime(),C) * alphas[5]
        outputStatsArr['waiting time'] = job.getWaitingTime()
        outputStatsArr['completed time'] = job.getCompletedTime()
        outputStatsArr['active time'] = job.getActiveTime()
        outputStatsArr['arrival time'] = job.getArrivalTime()
        outputStatsArr['assignment time'] = currentTime


        if ('predicted_qualities' in inputStatsArr):
            predicted_qualities = inputStatsArr['predicted_qualities']

            predictedQuality = ''

            if ((worker.getId(), job.getId()) not in predicted_qualities):
                print('Problem in combineAlgorithmDebugStatsArr: ' + str(worker.getId()) + ',' + str(job.getId()) + ' not in predicted_qualities.', file=f)
                predictedQuality = -999
            else:
                predictedQuality = predicted_qualities[(worker.getId(), job.getId())]

            outputStatsArr['quality predict'] = predictedQuality

            outputStatsArr['norm (alpha1 * quality prediction)'] = AlgorithmHelper.normalizeQuality(predictedQuality) * alphas[1]
        else:
            outputStatsArr['quality predict'] = -1
            outputStatsArr['norm (alpha1 * quality prediction)'] = -1




        if ('predicted_times' in inputStatsArr):

            predicted_times = inputStatsArr['predicted_times']

            predictedTime = ''

            if ((worker.getId(), job.getId()) not in predicted_times):
                print('Problem in predictTime: ' + str(worker.getId()) + ',' + str(job.getId()) + ' not in predicted_times.', file=f)
                predictedTime = -999
            else:
                predictedTime = predicted_times[(worker.getId(), job.getId())]

            outputStatsArr['time predict'] = predictedTime
            outputStatsArr['norm (alpha0 * time prediction)'] = AlgorithmHelper.normalizeTime(predictedTime) * alphas[0]

            if ('waiting_match' in inputStatsArr):
                waiting_match = inputStatsArr['waiting_match']
            else:
                waiting_match = {}

            outputStatsArr['predict value func'] = AlgorithmHelper.getAlgorithmValue(C,job, worker, scheduler, alphas, epsilon_precent, type_val_func, isWT, currentTime, predicted_times, predicted_qualities, waiting_match)
        else:
            outputStatsArr['time predict'] = -1
            outputStatsArr['norm (alpha0 * time prediction)'] = -1
            outputStatsArr['predict value func'] = -1




        if ('h_time' in inputStatsArr):
            outputStatsArr['h_time'] = inputStatsArr['h_time']
        else:
            outputStatsArr['h_time'] = -1


        if ('free_workers' in inputStatsArr):
            outputStatsArr['available workers'] = inputStatsArr['free_workers']
        else:
            outputStatsArr['available workers'] = []



        # completion time:

        # Not possible here, because when job is finished, it's removed from the queue
        # It should be done separately though jobsManager.getCompletedJobsList() method or on job completion event
        
        if job.getCompletedTime() > 0:
            outputStatsArr['real time'] = AlgorithmHelper.calculateRealTime(job, worker, scheduler)
            outputStatsArr['norm (alpha0 * real time)'] = AlgorithmHelper.normalizeTime(AlgorithmHelper.calculateRealTime(job, worker, scheduler)) * alphas[0]
            outputStatsArr['real quality'] = AlgorithmHelper.calculateRealQuality(job, worker, scheduler)
            print (AlgorithmHelper.calculateRealQuality(job, worker, scheduler), file=f)
            print(job)
            print(worker)
            print(scheduler)
            outputStatsArr['norm (alpha1 * quality)'] = AlgorithmHelper.normalizeQuality(AlgorithmHelper.calculateRealQuality(job, worker, scheduler)) * alphas[1]
            outputStatsArr['eval func'] = AlgorithmHelper.getAlgorithmEvaluation(job, worker, alphas, type_val_func, scheduler,C)
            outputStatsArr['total time'] = job.getWaitingTime() + AlgorithmHelper.calculateRealTime(job, worker, scheduler)
        else:
            outputStatsArr['real time'] = -1
            outputStatsArr['norm (alpha0 * real time)'] = -1
            outputStatsArr['real quality'] = -1
            outputStatsArr['norm (alpha1 * quality)'] = -1
            outputStatsArr['eval func'] = -1
            outputStatsArr['total time'] = -1

        return outputStatsArr
