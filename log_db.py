import pymongo
import config as cfg

class LOG_Contoller():
    def __init__(self,cfg) -> None:
        client = pymongo.MongoClient(host=cfg.mongohost,port=cfg.mongoport)
        db = client[cfg.mongodb]
        self.log = db[cfg.log]
        self.label = db[cfg.label_collection]

    def insert_error(self,task,id,errorType,labeler,correct_order=''):
        if errorType == 1:
            item = self.label.find_one({'task':task,'id':id})
            temp = {
                'task':task,
                'id':id,
                'order':item['order'],
                'errorType':errorType,
                'correct_order':correct_order,
                'labeler':labeler
            }
            self.log.insert_one(temp)
        else:
            temp = {
                'task':task,
                'id':id,
                'errorType':errorType,
                'labeler':labeler
            }
            self.log.insert_one(temp)

    def find_ordererror(self):
        temp = self.log.find({'errorType':1})
        res = [item for item in temp]
        return res

if __name__=="__main__":
    db = LOG_Contoller(cfg=cfg)
    # db.insert_error('test1',11,1,'fei','test')
    res = db.find_ordererror()
    print(res)