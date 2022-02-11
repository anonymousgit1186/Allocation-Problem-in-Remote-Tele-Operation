import json

from Vehicle import *
from BaseManager import BaseManager

class VehiclesManager:

    def __init__(self):

        self.mVehiclesList = {}

    @staticmethod
    def getVehicleDescriptionFromXml(xmlObject):

        emptyVehicleDescription = Vehicle.toArray(Vehicle(''))

        return BaseManager.getObjectDescriptionFromXml(xmlObject, emptyVehicleDescription)

    def addVehiclesFromXML(self, lVehiclesXMLNode):

        for vehicle in lVehiclesXMLNode:

            vehicleDescription = VehiclesManager.getVehicleDescriptionFromXml(vehicle)

            self.addVehicle(vehicleDescription)


    def addVehicle(self, lVehicleDescriptionArr):

        newVehicleId = lVehicleDescriptionArr['id']

        vehicle = Vehicle(newVehicleId)
        vehicle.loadFromArr(lVehicleDescriptionArr)

        self.mVehiclesList[newVehicleId] = vehicle

    def getVehicleById(self, lId):
        
        if (lId not in self.mVehiclesList):
            return None
            
        return self.mVehiclesList[lId]

    def getVehicleByName(self, lVehicleName):
        
        raise NotImplementedError

    def getVehiclesList(self):
        
        return self.mVehiclesList
