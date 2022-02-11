import sys
import time
from collections import Counter

import pandas as pd
import numpy as np
import scipy.stats as stats
import matplotlib 
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import random
import networkx as nx
from munkres import Munkres, make_cost_matrix, print_matrix
import os
from Base.Assignment import Assignment

sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'Algorithm'))
from Algorithm.AlgorithmHelper import AlgorithmHelper

# threshold = 0
# delta = 0.015
scenarios_dict = {"scenario0": "park_scenario_a", "scenario1": "turn_left_scenario_a",
                  "scenario2": "slow_vehicle_scenario_a", "scenario3": "foreign_object_scenario_a",
                  "scenario4": "extreme_weather_scenario_a"}

OUTPUT_DIR_NAME = 0

COL_SUFFIX_TIME = "_time_participant"
COL_SUFFIX_QUALITY = "_quality"
COL_PARTICIPANT_ID = 'participantId'


def add_to_df(writer, df_, currentTime):
    df_.to_excel(writer, sheet_name='tick ' + str(currentTime))


def Hungarian(C, B_graph, JobManager, alphas, eps, type_val_func, currentTime, writer, isWT, predicted_times,
              predicted_qualities, statistics):
    g = nx.Graph()

    workers, jobs = B_graph[0], B_graph[1]
    if len(workers) == 0 or len(jobs) == 0:
        return []

    min_val1 = sys.maxsize

    wc = 0
    jc = 0
    start = True
    jobss = set()

    for IDworkwe, worker in workers.items():
        wc += 1

        for IDjob, job in jobs.items():

            jobss.add((job.getId(), job.getWaitingTime()))
            if start:
                jc += 1
            if 'constrains' in type_val_func:
                if AlgorithmHelper.predictQuality(job, worker, predicted_qualities,
                                                  eps) < AlgorithmHelper.ALGORITHM_THRESHOLD:  # constrains of quality
                    continue

            val1 = AlgorithmHelper.getAlgorithmValue(C, job, worker, JobManager, alphas, eps, type_val_func, isWT,
                                                     currentTime,
                                                     predicted_times, predicted_qualities, None)

            min_val1 = min(min_val1, val1)

            g.add_node(IDworkwe, bipartite=1)
            g.add_node(IDjob, bipartite=0)
            g.add_edge(IDworkwe, IDjob, initial_weight=val1)
        start = False

    tempLogDict = {}

    for edge in g.edges(data=True):
        g[edge[0]][edge[1]]['weight'] = max(edge[2]['initial_weight'], edge[2]['initial_weight'] - min_val1)
        if 'job' in edge[0]:
            jobNode = edge[0]
            workerNode = edge[1]
        else:
            jobNode = edge[1]
            workerNode = edge[0]

        if (workerNode not in tempLogDict):
            tempLogDict[workerNode] = {}

        tempLogDict[workerNode][jobNode] = max(edge[2]['initial_weight'], edge[2]['initial_weight'] - min_val1)


    statistics.writeAllocationValuesLog(tempLogDict, currentTime)

    matching1 = nx.algorithms.matching.max_weight_matching(g, weight='weight')

    top_nodes = {n for n, d in g.nodes(data=True) if d["bipartite"] == 0}
    mjw1 = []
    ew1 = 0
    for (x, y) in matching1:
        ew1 += g[x][y]['weight']
        if x in top_nodes:
            mjw1.append((x, y))
        else:
            mjw1.append((y, x))
    return mjw1


def assign_new_vertex(C, workers, jobs, scheduler, alphas, eps, type_val_func, queue_type, currentTime, writer, isWT,
                      predicted_times, predicted_qualities, statistics):
    pairs = []
    waiting_match = {}
    start_time = time.time()

    M_v = Hungarian(C, (workers, jobs), scheduler, alphas, eps, type_val_func, currentTime, writer, isWT,
                    predicted_times, predicted_qualities, statistics)
    end_time = time.time()

    for pair in M_v:
        w, j = workers[pair[1]], jobs[pair[0]]
        if w.isAvailable(currentTime):
            pairs.append((j, w))
        elif queue_type == 'personal':
            waiting_match[w.getId()] = []
            waiting_match[w.getId()].append(j.getId())
    return pairs, waiting_match, end_time - start_time


def algo1_add_waiting(all_jobs, all_workers, waiting_match, currentTime):
    pairs = []
    to_delete = []

    # if job has been waiting for busy worker, assign them now
    if waiting_match:
        for workerID, list_jobs in waiting_match.items():
            for jobID in list_jobs:
                if all_workers[workerID].isAvailable(currentTime):
                    if jobID in all_jobs.keys() and not all_jobs[jobID].isAssigned():
                        pairs.append((all_jobs[jobID], all_workers[workerID]))
                        del all_jobs[jobID]
                        list_jobs.remove(jobID)
                        break
    return pairs


def reject_jobs(not_assigned, C, jobsManager, all_workers, statistics, scheduler, alpha, eps, type_value_func, isWT,
                currentTime, predicted_times, predicted_qualities, waiting_match):
    jobs_avg_value = {}
    for job in not_assigned:
        value = 0
        for worker_ID, worker in all_workers.items():
            value += AlgorithmHelper.getAlgorithmValue(C, job, worker, scheduler, alpha, eps, type_value_func, isWT,
                                                       currentTime, predicted_times, predicted_qualities, waiting_match)
        jobs_avg_value[job] = value / len(all_workers.keys())

    selectedJob = min(jobs_avg_value, key=jobs_avg_value.get)
    statistics.writeRejectedJob(currentTime, selectedJob, 'notAssignedYet', 'notAssignedYet', 'long queue')
    jobsManager.removeJob(selectedJob.getId(), False)


def algo1(all_jobs, all_workers, currentTime, scheduler, alphas, epsilon, type_val_func, waiting_match,
          queue_type, writer, isWT, predicted_times, predicted_qualities, C, jobsManager, statistics,
          isJustBackToOnline):
    new_v_ID = None
    pairs = []

    for jobID, job in all_jobs.items():
        if job.getArrivalTime() == currentTime and not job.isAssigned():
            new_v_ID = jobID
            break

    if not new_v_ID:
        for workerID, worker in all_workers.items():
            if worker.getCompletedTime() + 1 == currentTime or isJustBackToOnline:
                if worker.isAvailable(currentTime):
                    new_v_ID = workerID
                    break

    if not new_v_ID:
        return pairs, waiting_match, None

    new_pairs, waiting_match, hungarian_time = assign_new_vertex(C, all_workers, all_jobs, scheduler, alphas, epsilon,
                                                                 type_val_func,
                                                                 queue_type, currentTime, writer, isWT, predicted_times,
                                                                 predicted_qualities, statistics)

    pairs.extend(new_pairs)

    return pairs, waiting_match, hungarian_time


def greedy(all_jobs_dict, all_workers_dict, scheduler, alphas, epsilon, type_val_func, isWT, currentTime,
           predicted_times, predicted_qualities, C, jobsManager, statistics, waiting_match):
    all_jobs = dict(all_jobs_dict)
    all_workers = dict(all_workers_dict)
    pairs = []

    for i in range(len(all_jobs)):

        matches_values = {}

        for workerID, worker in all_workers.items():
            for jobID, job in all_jobs.items():
                if worker.isAvailable(currentTime) and not job.isAssigned():
                    if 'constrains' in type_val_func:
                        if AlgorithmHelper.predictQuality(job, worker, predicted_qualities,
                                                          epsilon) < AlgorithmHelper.ALGORITHM_THRESHOLD:
                            continue

                    value = AlgorithmHelper.getAlgorithmValue(C, job, worker, scheduler, alphas, epsilon, type_val_func,
                                                              isWT, currentTime,
                                                              predicted_times, predicted_qualities, None)
                    matches_values[(job, worker)] = value

        if len(matches_values) == 0:
            break

        best_pair = max(matches_values, key=matches_values.get)
        pairs.append(best_pair)

        del all_workers[best_pair[1].getId()]
        del all_jobs[best_pair[0].getId()]


    return pairs, None


def random_greedy(all_jobs_dict, all_workers_dict, scheduler, alphas, epsilon, type_val_func, isWT, currentTime,
                  predicted_times, predicted_qualities, C):
    all_jobs = dict(all_jobs_dict)
    all_workers = dict(all_workers_dict)
    pairs = []

    for i in range(len(all_jobs)):

        selected = None
        matches_values = {}

        for workerID, worker in all_workers.items():
            for jobID, job in all_jobs.items():
                if worker.isAvailable(currentTime) and not job.isAssigned():
                    if 'constrains' in type_val_func:
                        if AlgorithmHelper.predictQuality(job, worker, predicted_qualities,
                                                          epsilon) < AlgorithmHelper.ALGORITHM_THRESHOLD:
                            continue

                    value = AlgorithmHelper.getAlgorithmValue(C, job, worker, scheduler, alphas, epsilon, type_val_func,
                                                              isWT, currentTime,
                                                              predicted_times, predicted_qualities, None)

                    matches_values[(job, worker)] = value

        if len(matches_values) == 0:
            break

        max_1 = max(matches_values, key=matches_values.get)

        del matches_values[max_1]

        if len(matches_values) != 0:
            max_2 = max(matches_values, key=matches_values.get)
            selected = random.choice([max_1, max_2])
        else:
            selected = max_1

        pairs.append(selected)

        del all_jobs[selected[0].getId()]
        del all_workers[selected[1].getId()]

    return pairs, None


def assign_pairs(C, pairs, scheduler, alphas, epsilon_precent, type_val_func, df_ass, isWT, currentTime, all_workers,
                 sampled_times, predicted_times, predicted_qualities, waiting_match, statistics,f, h_time=0):
    
    print("f is ", f, file=f)
    for pair in pairs:
        selectedJob, selectedWorker = pair[0], pair[1]
        jobTime = AlgorithmHelper.predictTime(selectedJob, selectedWorker, predicted_times, getSimulatedTime=True)

        # create assignment
        newAssignment = Assignment(selectedJob.getId(), selectedWorker.getId(),
                                   selectedJob.getArrivalTime(),
                                   jobTime + 1, currentTime + 1)

        scheduler.addAssignment(newAssignment)
        free_workers = AlgorithmHelper.getFreeWorkers(all_workers, currentTime)

        inputStatsArr = {}
        inputStatsArr['job'] = selectedJob
        inputStatsArr['worker'] = selectedWorker
        inputStatsArr['scheduler'] = scheduler
        inputStatsArr['alphas'] = alphas
        inputStatsArr['epsilon_precent'] = epsilon_precent
        inputStatsArr['type_val_func'] = type_val_func
        inputStatsArr['isWT'] = isWT
        inputStatsArr['currentTime'] = currentTime
        inputStatsArr['free_workers'] = free_workers
        inputStatsArr['sampled_times'] = sampled_times
        inputStatsArr['predicted_times'] = predicted_times
        inputStatsArr['predicted_qualities'] = predicted_qualities
        inputStatsArr['waiting_match'] = waiting_match
        inputStatsArr['h_time'] = h_time

        statsArr = statistics.combineAlgorithmDebugStatsArr(inputStatsArr, C,f, 'a+')

        statistics.writeAlgorithmDebugRow(statsArr, False, 'a+')

        df_ass = df_ass.append(statsArr, ignore_index=True)


        selectedJob.setIsAssigned(selectedWorker.getId(), currentTime + 1)
        selectedWorker.setAssignedJobId(selectedJob.getId())

        selectedWorker.setJobAssignedTime(currentTime + 1)
        selectedWorker.setBusyStartTime(currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

        scheduler.updateJobFetchTime(selectedJob.getId(), selectedJob, currentTime + 1)

        scheduler.updateJobStartTime(selectedJob.getId(), selectedJob, currentTime + 1)

        statsArr = {}
        statsArr['currentTick'] = currentTime
        statsArr['jobId'] = selectedJob.getId()
        statsArr['workerId'] = selectedWorker.getId()
        statsArr['jobArrivalTime'] = selectedJob.getArrivalTime()
        statsArr['jobTime'] = jobTime
        statsArr['freeWorkersCount'] = len(free_workers)

        statistics.writeAssignmentLog(statsArr, False, 'a+')

    return df_ass


def assign_exact_pairs(allJobs, allWorkers, scheduler, currentTime, statistics, isSimulationMode):
    for job in allJobs.values():

        for worker in allWorkers.values():

            # Works only with dedicated operators specified
            if (job.getDedicatedOperatorIndex() < 0):
                continue

            # Operator's index must be the same as job's dedicated index
            if (job.getDedicatedOperatorIndex() != worker.getIndex()):
                continue

            selectedJob = job
            selectedWorker = worker

            jobDuration = 999

            if (isSimulationMode):
                jobDuration = random.randint(job.getMinTimeForHandling(), job.getMaxTimeForHandling())

            # create assignment
            newAssignment = Assignment(selectedJob.getId(), selectedWorker.getId(),
                                       selectedJob.getArrivalTime(),
                                       jobDuration + 1, currentTime + 1)
            scheduler.addAssignment(newAssignment)
            free_workers = AlgorithmHelper.getFreeWorkers(allWorkers, currentTime)

            # Mark job and worker as not available
            selectedJob.setIsAssigned(selectedWorker.getId(), currentTime + 1)
            selectedWorker.setAssignedJobId(selectedJob.getId())

            selectedWorker.setJobAssignedTime(currentTime + 1)
            selectedWorker.setBusyStartTime(currentTime + 1 + AlgorithmHelper.JOB_LOADING_TICKS)

            statsArr = {}
            statsArr['currentTick'] = currentTime
            statsArr['jobId'] = selectedJob.getId()
            statsArr['workerId'] = selectedWorker.getId()
            statsArr['jobArrivalTime'] = selectedJob.getArrivalTime()

            statistics.writeAssignmentLog(statsArr, False, 'a+')

    return {}


def waiting_greedy(all_jobs_dict, all_workers_dict, scheduler, alphas, epsilon, type_val_func, isWT, currentTime,
                   predicted_times, predicted_qualities, waiting_match, C):
    all_jobs = dict(all_jobs_dict)
    all_workers = dict(all_workers_dict)

    pairs = []
    for i in range(len(all_jobs)):
        matches_values = {}
        for workerID, worker in all_workers.items():
            for jobID, job in all_jobs.items():
                if not job.isAssigned():
                    if 'constrains' in type_val_func:
                        if AlgorithmHelper.predictQuality(job, worker, predicted_qualities,
                                                          epsilon) < AlgorithmHelper.ALGORITHM_THRESHOLD:
                            continue

                    value = AlgorithmHelper.getAlgorithmValue(C, job, worker, scheduler, alphas, epsilon, type_val_func,
                                                              isWT, currentTime,
                                                              predicted_times, predicted_qualities, waiting_match)

                    matches_values[(job, worker)] = value

        if len(matches_values) == 0:
            break

        best_pair = max(matches_values, key=matches_values.get)
        pairs.append(best_pair)

        all_jobs[best_pair[0].getId()].setIsAssigned()


        bestWorker, bestJob = best_pair[1], best_pair[0]

        if not bestWorker.isAvailable(currentTime):
            if not bestWorker.getId() in waiting_match.keys():
                waiting_match[bestWorker.getId()] = []
            waiting_match[bestWorker.getId()].append(bestJob.getId())

    return pairs, waiting_match


def run_algo(algo, all_jobs, all_workers, waiting_match, scheduler, currentTime, JobsManager, WorkersManager, alphas,
             epsilon, type_val_func, writer, df_ass, isWT, sampled_times, predicted_times, predicted_qualities, C,
             jobsManager,
             num_workers,
             scenario_number, num_jobs, statistics, isSimulationMode, isJustBackToOnline, f):
    h_time_threshold = 0
    global OUTPUT_DIR_NAME

    if OUTPUT_DIR_NAME == 0:
        OUTPUT_DIR_NAME = "{}_workers/scenario_{}".format(num_workers, scenario_number)

    h_times = []

    if 'algo1' in algo:
        if 'personal' in algo:
            queue_type = 'personal'
        else:
            queue_type = 'general'

        pairs = algo1_add_waiting(all_jobs, all_workers, waiting_match, currentTime)
        df_ass = assign_pairs(C, pairs, scheduler, alphas, epsilon, type_val_func, df_ass, isWT, currentTime,
                              all_workers,
                              sampled_times, predicted_times, predicted_qualities, None, statistics,f)
        if 'threshold' in algo:
            assign_now, available_now = AlgorithmHelper.getJobsWorkersForThreshold(all_jobs, all_workers, currentTime)
            if len(available_now) > 0:
                pairs, waiting_match, h_time_threshold = assign_new_vertex(C, available_now, assign_now, scheduler,
                                                                           alphas,
                                                                           epsilon,
                                                                           type_val_func, queue_type,
                                                                           currentTime, writer, isWT, predicted_times,
                                                                           predicted_qualities, statistics)
                df_ass = assign_pairs(C, pairs, scheduler, alphas, epsilon, type_val_func, df_ass, isWT, currentTime,
                                      all_workers, sampled_times, predicted_times, predicted_qualities, None,
                                      statistics, f)

        pairs, waiting_match, h_time = algo1(all_jobs, all_workers, currentTime, scheduler, alphas,
                                             epsilon, type_val_func, waiting_match, queue_type, writer, isWT,
                                             predicted_times, predicted_qualities, C, jobsManager, statistics,
                                             isJustBackToOnline)

        df_ass = assign_pairs(C, pairs, scheduler, alphas, epsilon, type_val_func, df_ass, isWT, currentTime,
                              all_workers,
                              sampled_times, predicted_times, predicted_qualities, None, statistics,f, h_time )

        if h_time_threshold:
            h_times.append(h_time_threshold)
        if h_time:
            h_times.append(h_time)

        return waiting_match, df_ass, h_times

    elif 'greedy' == algo:

        pairs, _ = greedy(all_jobs, all_workers, scheduler, alphas,
                          epsilon, type_val_func, isWT, currentTime, predicted_times, predicted_qualities, C,
                          jobsManager, statistics, waiting_match)

        df_ass = assign_pairs(C, pairs, scheduler, alphas, epsilon, type_val_func, df_ass, isWT, currentTime,
                              all_workers,
                              sampled_times, predicted_times, predicted_qualities, None, statistics, f)
        return None, df_ass, h_times

    elif 'training_algo' == algo:

        assign_exact_pairs(all_jobs, all_workers, scheduler, currentTime, statistics, isSimulationMode)
        df_ass = pd.DataFrame()
        return None, df_ass, h_times

    elif 'random_greedy' == algo:

        pairs, _ = random_greedy(all_jobs, all_workers, scheduler, alphas,
                                 epsilon, type_val_func, isWT, currentTime, predicted_times, predicted_qualities, C)

        df_ass = assign_pairs(C, pairs, scheduler, alphas, epsilon, type_val_func, df_ass, isWT, currentTime,
                              all_workers,
                              sampled_times, predicted_times, predicted_qualities, None, statistics,f)

        return None, df_ass, h_times

    elif 'waiting_greedy':

        pairs = algo1_add_waiting(all_jobs, all_workers, waiting_match, currentTime)

        df_ass = assign_pairs(C, pairs, scheduler, alphas, epsilon, type_val_func, df_ass, isWT, currentTime,
                              all_workers,
                              sampled_times, predicted_times, predicted_qualities, waiting_match, statistics,f)

        pairs, waiting_match = waiting_greedy(all_jobs, all_workers, scheduler, alphas, epsilon, type_val_func, isWT,
                                              currentTime, predicted_times, predicted_qualities, waiting_match, C)

        df_ass = assign_pairs(C, pairs, scheduler, alphas, epsilon, type_val_func, df_ass, isWT, currentTime,
                              all_workers,
                              sampled_times, predicted_times, predicted_qualities, waiting_match, statistics,f)

        return waiting_match, df_ass, h_times


def print_allocation_value(statistics, scheduler, JobManager, WorkerManager, alphas, epsilon_precent, type_val_func,
                           isWT, algo_type, currentTime, C, f):
    print("====================END==================",file=f)
    alloc_val = 0
    assignments = scheduler.getCompletedAssignments()
    assigned_workers = []

    df_ass = pd.DataFrame()

    for i, (assignmentID, assignment) in enumerate(assignments.items()):
        completedJob = JobManager.getJobById(assignment.getJobId())
        completedJobWorker = WorkerManager.getWorkerById(assignment.getWorkerId())

        inputStatsArr = {}
        inputStatsArr['job'] = completedJob
        inputStatsArr['worker'] = completedJobWorker
        inputStatsArr['scheduler'] = scheduler
        inputStatsArr['alphas'] = alphas
        inputStatsArr['epsilon_precent'] = epsilon_precent
        inputStatsArr['type_val_func'] = type_val_func
        inputStatsArr['isWT'] = isWT
        inputStatsArr['currentTime'] = -1

        statsArr = statistics.combineAlgorithmDebugStatsArr(inputStatsArr, C,f, 'a+')

        statistics.writeAlgorithmDebugRow(statsArr, False, 'a+')

        df_ass = df_ass.append(statsArr, ignore_index=True)

        row = df_ass.loc[df_ass['job ID'] == completedJob.getId()]
        df_value = row['eval func'].values[0]

        val = AlgorithmHelper.getAlgorithmEvaluation(completedJob, completedJobWorker, alphas, type_val_func,
                                                     scheduler, C)
        alloc_val += val  # float(df_value)
        # print("match", i, ": ", worker.getId(), "->", j.getId())
        assigned_workers.append(completedJobWorker.getId())

    mean_tt = 0
    max_tt = 0

    if ('total time' in df_ass):
        mean_tt = df_ass['total time'].mean()
        max_tt = df_ass['total time'].max()

    c = Counter(assigned_workers)
    print("epsilon percent:", epsilon_precent,file=f)
    print("evaluation value: ", alloc_val,file=f)
    print("num of assignments: ", len(assignments.keys()),file=f)
    print("workers assigned: ", c,file=f)
    print("alphas: ", alphas,file=f)
    print("type of value function: ", type_val_func,file=f)
    print("algo type: ", algo_type,file=f)
    print('saving assignments...',file=f)
    WT = "" if isWT else "_noWT"

    return alloc_val, mean_tt, max_tt
