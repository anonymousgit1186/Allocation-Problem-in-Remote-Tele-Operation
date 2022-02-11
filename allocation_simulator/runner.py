import run_scheduler
import sys
import subprocess
import multiprocessing

runs = list()

with open("config.txt", "r") as cnf:
    for line in cnf : 
        runs.append(line.strip().split(" "))


runs_new = []

i = 0

for line in runs :
    solver_path = line[-2]
    target = str(i) + ".csv"

    p = subprocess.Popen(['scp', solver_path ,
                          target])

    sts = p.wait()

    runs_new.append(line[:-1])
    runs_new[-1][-1] = target

    i=i+1
    

with multiprocessing.Pool(min(len(runs_new), 94)) as p:
    res = p.map(run_scheduler.run, runs_new)

for item in res:
    print(item)
