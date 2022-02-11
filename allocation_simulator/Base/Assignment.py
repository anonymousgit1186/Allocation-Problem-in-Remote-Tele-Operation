from graphviz import Digraph


class Assignment:

    def __init__(self, lJobId, lWorkerId, lArrivalTime, lDuration, lActualStartTick, lAssignmentTick = -1):
        self.mJobId = lJobId
        self.mWorkerId = lWorkerId

        self.mArrivalTime = lArrivalTime

        self.mActiveTime = 0

        self.mActualStartTick = lActualStartTick
        self.mRemainedTime = lDuration

        self.mFetchTime = -1
        self.mStartTime = -1

        self.mAssignmentTick = lAssignmentTick

        self.mId = str(self.mJobId) + '_' + str(self.mWorkerId) + '_' + str(self.mArrivalTime) + '_' + str(
            lDuration)

    def getId(self):
        return self.mId

    def getActiveTime(self):
        return self.mActiveTime

    def getAssignmentTime(self):
        return self.mAssignmentTick

    def getArrivalTime(self):
        return self.mArrivalTime

    def setActiveTime(self, activeTime):
        self.mActiveTime = activeTime

    def isJobCompleted(self):
        if (self.mRemainedTime <= 0):
            return True

        return False

    def onTick(self, currentTime):

        if (self.mActualStartTick <= currentTime):
            self.mRemainedTime -= 1

            self.mActiveTime += 1

    def getJobId(self):
        return self.mJobId

    def getWorkerId(self):
        return self.mWorkerId

    def getRemaindTime(self):
        return self.mRemainedTime

    def setRemainedTime(self, remainedTime):
        self.mRemainedTime = remainedTime
    
    def getFetchTime(self):
        return self.mFetchTime

    def setFetchTime(self, fetchTime):
        self.mFetchTime = fetchTime
    
    def getStartTime(self):
        return self.mStartTime

    def setStartTime(self, startTime):
        self.mStartTime = startTime
    
    def dot(self, dot):
        jid = str(self.mJobId)
        wid = 'W ' + str(self.mWorkerId)

        dot.edge(wid, jid, color='red', constraint='False')

    def __str__(self):

        output = ''
        
        output += str(self.mJobId) + ' : '
        output += str(self.mWorkerId) + ' : '
        output += str(self.mArrivalTime) + ' : '
        output += str(self.mActiveTime) + ' : '
        output += str(self.mRemainedTime) + ' : '

        return output
