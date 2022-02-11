import json

class Vehicle:

    def __init__(self, lId, lName = '', lType = '', lModel = ''):
        
        self.mId = lId
        self.mName = lName
        self.mType = lType
        self.mModel = lModel

    def getId(self):
        return self.mId

    def getName(self):
        return self.mName

    def getType(self):
        return self.mType
        
    def getModel(self):
        return self.mModel
        

    def loadFromArr(self, jobDescriptionArr):

        self.mId = jobDescriptionArr['id']
        self.mName = jobDescriptionArr['name']

        self.mType = jobDescriptionArr['type']
        self.mModel = jobDescriptionArr['model']

    @staticmethod
    def toArray(vehicle):
        
        entityJson = {}

        entityJson['id'] = vehicle.getId()
        entityJson['name'] = vehicle.getName()

        entityJson['type'] = vehicle.getType()
        entityJson['model'] = vehicle.getModel()

        return entityJson

    @staticmethod
    def toJson(vehicle):
        
        return json.dumps(Vehicle.toArray(vehicle))
        