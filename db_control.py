import pymongo
import config as cfg
import pandas as pd
from werkzeug.security import generate_password_hash,check_password_hash

class DB_Contoller():
    def __init__(self,cfg) -> None:
        client = pymongo.MongoClient(host=cfg.mongohost,port=cfg.mongoport)
        db = client[cfg.mongodb]
        self.label = db[cfg.label_collection]
        self.labeler = db[cfg.labeler]

    def read_file(self,file_path):
        data = pd.read_excel(file_path)
        result = []
        for item in data.iloc():
            order = item['管制语音原文']
            order = ''.join(order.split())
            
            result.append(order)
        return result

    def manage_task(self,file_path,taskname,labelers):
        data = self.read_file(file_path=file_path)
        task_num = len(data)//len(labelers)
        for i in range(len(labelers)):
            self.add_labeler(labelers[i])
            if i+1==len(labelers):
                end = len(data)
            else:
                end = (i+1)*task_num
            situation = self.add_task(labelers[i],taskname,i*task_num,end-1)
            if situation != 'task add success':
                self.change_task(labelers[i],taskname,i*task_num,end)
        for i in range(len(data)):
            self.insert_LabelData(taskname,i,data[i])
        return True

    def get_unlabeledData(self,labeler,task,num=1,id=-1):
        if id != -1:
            access = self.check_task(name=labeler,task=task,id=id)
            if access == 'access':
                temp = self.get_LabelData(task=task,id=id)
                return [temp]
            else:
                return 'access refused'
        else:
            ler = self.labeler.find_one({"name":labeler})
            if ler:
                tasklist = ler['task']
                ids = None
                for titem in tasklist:
                    if task == titem['taskname']:
                        ids = titem['ids']
                if ids != None:
                    start = ids[0]
                    end = ids[1]
                    fil = {
                        "task":task,
                        "id":{"$gte":start,"$lte":end},
                        "tag":False
                    }
                    order = self.label.find(fil).limit(num)
                    result = [item for item in order]
                    return result
                else:
                    return f'{labeler} without {task} task'
            else:
                return f'{labeler} labeler not exist'


    def insert_LabelData(self,task,id,order):
        exist = self.label.find_one({"id":id,'task':task})
        if exist == None:
            temp = {
                'id':id,
                'task':task,
                'order':order,
                "intent":"",
                "sender":"",
                "tag":False,
                'label':[],
                'labeler':""
            }
            self.label.insert_one(temp)
            return True
        else:
            return False

    def get_LabelData(self,task,id):
        exist = self.label.find_one({"task":task,"id":id})
        return exist

    def get_label_table(self,name,task):
        lbr = self.labeler.find_one({'name':name})
        if lbr['rule'] == 'labeler':
            tasklist = lbr['task']
            for item in tasklist:
                if item['taskname'] == task:
                    start,end = item['ids']
                    fil = {
                        "task":task,
                        "id":{"$gte":start,"$lte":end}
                    }
                    order = self.label.find(fil,{"_id":0,"id":1,"order":1,"tag":1})
                    result = [item for item in order]
                    return result
            return {'error':'no task'}
        elif lbr['rule'] == 'admin':
            fil = {
                "task":task
            }
            order = self.label.find(fil,{"_id":0,"id":1,"order":1,"tag":1})
            result = [item for item in order]
            return result
        else:
            return {'error':'user error'}


    def delete_labelData(self,task,id):
        res = self.label.delete_one({"task":task,"id":id})
        return res

    def work_labelData(self,task,id,labeler,intent,sender,labelinfo,tag=True):
        access = self.check_task(labeler,task,id)
        if access == "access":
            if self.label.find_one({"task":task,"id":id}):
                upinfo = {
                    'labeler':labeler,
                    'label':labelinfo,
                    'tag':tag,
                    'sender':sender,
                    "intent":intent
                }
                res = self.label.update_one({"id":id,'task':task},{"$set":upinfo})
                print(res)
                res = self.label.find_one({"id":id,'task':task})
                res = str(res) + 'update'
                return res
            return f'{task}-{id} no exist'
        else:
            return access

    #-----------------------labeler-----------------------
    def add_labeler(self,name,password,rule='labeler'):
        item = self.labeler.find_one({'name':name})
        if item == None:
            labeler = {
                "name":name,
                "rule":rule,
                "password":generate_password_hash(password),
                "task":[]
            }
            self.labeler.insert_one(labeler)
            return True
        else:
            return False
    
    def add_task(self,name,task,start,end):
        exist = self.labeler.find_one({'name':name})
        if exist != None:
            tasklist = exist['task']
            for item in tasklist:
                if task == item['taskname']:
                    return f'{name} has {task} task'
            
            task_plan = {
                "taskname":task,
                "ids":[start,end]
            }
            tasklist.append(task_plan)
            self.labeler.update_one({'name':name},{"$set":{'task':tasklist}})
            return 'task add success'
        else:
            return '{name} labeler not exist'

    def change_task(self,name,task,start,end):
        exist = self.labeler.find_one({'name':name})
        if exist != None:
            tasklist = exist['task']
            for item in tasklist:
                if task == item['taskname']:
                    item['ids'] = [start,end]
                    break
            self.labeler.update_one({'name':name},{"$set":{'task':tasklist}})
            return 'task change success'
        else:
            return '{name} labeler not exist'

    def check_task(self,name,task,id):
        lbr = self.labeler.find_one({'name':name})
        if lbr:
            if lbr['rule'] == "admin":
                return 'access'
            else:
                tasklist = lbr['task']
                for item in tasklist:
                    if task == item['taskname']:
                        ids = item['ids']
                        if ids[0] <= id <= ids[1]:
                            return 'access'
                        else:
                            return f"{name} task-{task} id:{id} can not access"
                return f'{name} no {task} task'
        else:
            return f'{name} not exist'
    
    def check_labeler(self,name,password):
        lbr = self.labeler.find_one({'name':name})
        if lbr:
            if check_password_hash(lbr['password'],password):
                return 'access'
            else:
                return 'labeler or password error'
        else:
            return f'{name} not exist'

    def _set_labeler_password(self,name,newpassword='123456'):
        lbr = self.labeler.find_one({'name':name})
        if lbr:
            self.labeler.update_one({'name':name},{"$set":{'password':generate_password_hash(newpassword)}})
            return 'success'
        else:
            return f'{name} not exist'

if __name__=="__main__":
    db = DB_Contoller(cfg)
    # res = db.insert_LabelData('test',2,'你好南方六六幺五北京alphadelta六两三以后直飞alphadelta六四六')
    # res = db.get_LabelData('test',1)
    # res = db.add_labeler('fei')
    # res = db.add_task('fei','test',0,100)
    # res = db.get_unlabeledData('fei','test',1)
    # res = db.work_labelData('test',1,'fei','test','sender',[{'word':'12','BIO':'B-test'},{'word':'34','BIO':'I-test'},{'word':'56','BIO':'O'}])
    # res = db.change_task('fei','test',0,200)
    # res = db.delete_labelData('test',1)
    # res = db.check_task('feiw','test',1)
    # print(res)
    # db.manage_task('task/minhang.xlsx',taskname='test1',labelers=['fei','wang'])
    # res = db._set_labeler_password('wang')
    # res = db.check_labeler('wang','123456')
    # res = db.add_labeler('admin',password='admin',rule='admin')
    res = db.get_label_table('wang','test1')
    print(res)