import json
import os
import random
import sys

sys.dont_write_bytecode = True

from scipy import stats as stats
from scipy.stats import norm

from Base.Assignment import Assignment

sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'Algorithm'))
from Algorithm.AlgorithmHelper import AlgorithmHelper

from prepare_data import create_C_lambda_probs, create_C_lambda, create_pjt

import numpy as np


class LP_matching:

    @staticmethod
    def init_data(f, T, C, workersIdsList, tripDataDirectoryIndex='1'):
        pjt = create_pjt(T, C)
        print("done pjt", file=f)

        LHS = len(workersIdsList)
        RHS = len(pjt[0])
        C_lambda_dict = create_C_lambda(workersIdsList)
        C_lambda_probs = create_C_lambda_probs(LHS, RHS, T, C, C_lambda_dict)
        print("done C_lambda", file=f)

        return LHS, RHS, pjt, C_lambda_probs

    @staticmethod
    def solveLP(path):
        Xopt = {}

        # Example
        # path = LP_matching/pjt_stats_3_states_5_hours.csv
        with open(path, 'r') as fp:

            X_results = fp.readlines()
            for line in X_results:
                if 't' in line:
                    continue
                vals = line.split(',')
                t = int(vals[0])
                t_prime = int(vals[1])
                i = int(vals[2])
                j = int(vals[3])
                val = float(vals[4].rstrip('\n'))
                Xopt[(t, t_prime, i, j)] = val

        return Xopt

    @staticmethod
    def sampleAssignment(Xopt, LHS, t, j, pt, C, T):
        r = random.uniform(0, 1)
        cur_sum = 0
        for i in range(LHS):
            for t_tag in range(t, t + C):
                if t_tag > T:
                    break
                temp_u = Xopt[(t, t_tag, i, j)] / pt[j]
                cur_sum += temp_u
                if r <= cur_sum:
                    return i, t_tag

        return LHS - 1, t + C - 1

    @staticmethod
    def ATT(f, predicted_times, predicted_qualities, assignmentsList, alreadyAssignedJobsIndices, currentTime, waitingJobs,
            LHS, RHS, pjt, T,
            C_lambda_probs, competitiveRatio, C, Xopt, jobsManager, workersManager, isWT, type_val_func, scheduler,
            statistics):

        for jobId, job in waitingJobs.items():

            waitingJobIndex = jobsManager.getJobIndexByJobId(jobId)
            waitingJobTypeIndex = jobsManager.getJobTypeIndexByJobId(jobId)
            if (waitingJobIndex in alreadyAssignedJobsIndices):
                continue

            # cur_i - worker
            # t_tag - busy start time
            cur_i, t_tag = LP_matching.sampleAssignment(Xopt, LHS, currentTime, waitingJobTypeIndex,
                                                             pjt[currentTime],
                                                             C, T)

            assignmentsList[t_tag][cur_i].append((currentTime, t_tag, cur_i, waitingJobIndex))

            alreadyAssignedJobsIndices[waitingJobIndex] = True

        for i in range(LHS):

            workerI = workersManager.getWorkerByIndex(i)

            assignment = LP_matching.SP(f,jobsManager, workerI, i, currentTime, assignmentsList[currentTime][i],
                                             competitiveRatio, C, Xopt, C_lambda_probs, RHS, scheduler, statistics)

            if not assignment:
                continue

            # t_ass is the arrival time
            # t_tag_ass is the assignment time
            t_ass, t_tag_ass, workerIndex, jobIndex = assignment

            selectedWorker = workersManager.getWorkerByIndex(workerIndex)
            selectedJob = jobsManager.getJobByIndex(jobIndex)

            # Predicted run time for this job
            jobTime = AlgorithmHelper.predictTime(selectedJob, selectedWorker, predicted_times,
                                                  getSimulatedTime=True)

            # create assignment
            newAssignment = Assignment(selectedJob.getId(), selectedWorker.getId(),
                                       selectedJob.getArrivalTime(),
                                       jobTime + 1, currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS,
                                       currentTime + 1)

            scheduler.addAssignment(newAssignment)

            selectedJob.setIsAssigned(selectedWorker.getId(), currentTime + 1)
            selectedWorker.setAssignedJobId(selectedJob.getId())

            selectedWorker.setJobAssignedTime(currentTime + 1)
            selectedWorker.setBusyStartTime(currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

            scheduler.updateJobFetchTime(selectedJob.getId(), selectedJob, currentTime + 1)

            scheduler.updateJobStartTime(selectedJob.getId(), selectedJob,
                                         currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

            free_workers = AlgorithmHelper.getFreeWorkers(workersManager.getWorkersList(), currentTime)

            inputStatsArr = {}
            inputStatsArr['job'] = selectedJob
            inputStatsArr['worker'] = selectedWorker
            inputStatsArr['scheduler'] = scheduler
            inputStatsArr['type_val_func'] = type_val_func
            inputStatsArr['isWT'] = isWT
            inputStatsArr['currentTime'] = currentTime
            inputStatsArr['free_workers'] = free_workers
            inputStatsArr['predicted_times'] = predicted_times
            inputStatsArr['predicted_qualities'] = predicted_qualities

            statsArr = statistics.combineAlgorithmDebugStatsArr(inputStatsArr, C, f, 'a+')

            statistics.writeAlgorithmDebugRow(statsArr, False, 'a+')

            statsArr = {}
            statsArr['currentTick'] = currentTime
            statsArr['jobId'] = selectedJob.getId()
            statsArr['workerId'] = selectedWorker.getId()
            statsArr['jobArrivalTime'] = selectedJob.getArrivalTime()
            statsArr['jobTime'] = jobTime
            statsArr['freeWorkersCount'] = len(free_workers)

            statistics.writeAssignmentLog(statsArr, False, 'a+')

        return assignmentsList, alreadyAssignedJobsIndices

    @staticmethod
    def calc_qit(cur_i, t, competitiveRatio, C, Xopt, C_lambda, RHS):
        cur_sum = 0
        for t_arr in range(t):
            for t_tag in range(t_arr, t_arr + C):
                if t_tag >= t:
                    continue
                for j in range(RHS):
                    usage_time = t - t_tag
                    cur_sum += competitiveRatio * Xopt[(t_arr, t_tag, cur_i, j)] * C_lambda[cur_i][j][usage_time]
        return 1 - cur_sum

    @staticmethod
    # cur_i - workerIndex
    def SP(f,jobsManager, worker, cur_i, t, Ati, competitiveRatio, C, Xopt, C_lambda, RHS, scheduler, statistics):

        # if there is no assignments in queue
        if len(Ati) == 0:
            return None

        isCurrentJobIsGoingToEndToday = False

        if (not worker.isAvailable(t)):
            workerCurrentJobId = worker.getAssignedJobId()
            assignment = scheduler.getAssignmentByJobId(workerCurrentJobId)

            isCurrentJobIsGoingToEndToday = assignment.getRemaindTime() <= 1

        # extract is i busy
        if not worker.isAvailable(t) and not isCurrentJobIsGoingToEndToday:
            for ass in Ati:
                jobIndex = ass[3]
                selectedJob = jobsManager.getJobByIndex(jobIndex)
                statistics.writeRejectedJob(t, selectedJob, worker.getId(), ass[1], 'worker_is_unavailable')
                jobsManager.removeJob(selectedJob.getId(), False)
            return None

        qit = LP_matching.calc_qit(cur_i, t, competitiveRatio, C, Xopt, C_lambda, RHS)
        t_tag_lower_bound = 0 if t + 1 - C < 0 else t + 1 - C
        for t_tag in range(t_tag_lower_bound, t + 1):
            # for t_tag in range(t + 1):
            if qit < 0.5:
                qit = 0.5

            Zt_tag = np.random.choice([0, 1], p=[(1 - (competitiveRatio / qit)), (competitiveRatio / qit)])
            for index, ass in enumerate(Ati):
                if ass[0] == t_tag:
                    if (Zt_tag):
                        for ind, reject_ass in enumerate(Ati):
                            if ind != index:
                                jobIndex = reject_ass[3]
                                selectedJob = jobsManager.getJobByIndex(jobIndex)
                                if (None != selectedJob):
                                    statistics.writeRejectedJob(t, selectedJob, worker.getId(), reject_ass[1],
                                                                'other_job_from_Ati_was_chosen')
                                    jobsManager.removeJob(selectedJob.getId(), False)
                        return ass
                    else:
                        jobIndex = ass[3]
                        selectedJob = jobsManager.getJobByIndex(jobIndex)
                        statistics.writeRejectedJob(t, selectedJob, worker.getId(), ass[1], 'Zt_prime=0')
                        jobsManager.removeJob(selectedJob.getId(), False)

            qit = qit - competitiveRatio * sum([Xopt[(t_tag, t, cur_i, j)] for j in range(RHS)])
        return None

    @staticmethod
    def runAlgorithm(f,predicted_times, predicted_qualities, assignmentsList, alreadyAssignedJobsIndices, currentTime,
                     firstWaitingJob, LHS,
                     RHS, pjt, T, C_lambda_probs,
                     competitiveRatio, C, Xopt, jobsManager, workersManager, isWT, type_val_func, scheduler,
                     statistics):

        return LP_matching.ATT(f,predicted_times, predicted_qualities, assignmentsList, alreadyAssignedJobsIndices,
                                    currentTime,
                                    firstWaitingJob, LHS, RHS, pjt, T,
                                    C_lambda_probs, competitiveRatio, C, Xopt, jobsManager, workersManager, isWT,
                                    type_val_func, scheduler,
                                    statistics)