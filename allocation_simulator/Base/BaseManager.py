import json
from xml.etree import ElementTree

class BaseManager:

    def __init__(self):

        pass

    @staticmethod
    def getObjectDescriptionFromXml(xmlObject, emptyObjectDescription):

        entityJson = {}

        for objectDescriptionKey in emptyObjectDescription:

            try:
                entityJson[objectDescriptionKey] = xmlObject.get(objectDescriptionKey)
            except:
                pass    

            try:
                objectDescriptionKeyNode = xmlObject.find(objectDescriptionKey)

                children = list(objectDescriptionKeyNode)

                if (len(children) > 0):

                    entityJson[objectDescriptionKey] = {}
                    
                    for child in children:
                        entityJson[objectDescriptionKey][child.tag] = child.text
                else:
                    entityJson[objectDescriptionKey] = objectDescriptionKeyNode.text

            except Exception as e:
                # print(e)
                pass

        return entityJson
