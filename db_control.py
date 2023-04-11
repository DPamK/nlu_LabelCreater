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
            id = item['序号']
            id = int(id)
            order = item['通话']
            order = str(order)
            order = ''.join(order.split())
            
            result.append([id,order])
        return result

    def manage_task(self,file_path,taskname,labelers):
        data = self.read_file(file_path=file_path)
        task_num = len(data)//len(labelers)
        for i in range(len(labelers)):
            lbr = self.labeler.find_one({"name":labelers[i]})
            if lbr == None:
                return 'labeler {labelers[i]} not exist'
            else:
                if i+1==len(labelers):
                    end = len(data)
                else:
                    end = (i+1)*task_num
                start_id = data[i*task_num][0]
                end_id = data[end-1][0]
                situation = self.add_task(labelers[i],taskname,start_id,end_id)
                if situation != 'task add success':
                    self.change_task(labelers[i],taskname,i*task_num,end)

        for i in range(len(data)):
            id,order = data[i]
            self.insert_LabelData(task=taskname,id=id,order=order)
        self.admin_add_task('admin',task=taskname,labeler=labelers)
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
                "intent":[],
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

    def get_tasklist(self,name):
        lbr = self.labeler.find_one({'name':name})
        if lbr['rule'] == 'labeler':
            tasklist = lbr['task']
            result = []
            for task in tasklist:
                result.append(task['taskname'])
            return {"tasklist":result}
        elif lbr['rule'] == 'admin':
            result = {'tasklist':lbr['task']}
            return result
        else:
            return {'error':'user error'}

    def _get_data(self,tasklist):
        def get_newitem(item):
            abbr = ""
            detail = ""
            if 'discard' in item and item['discard'] == True:
                tag = '废弃'
            elif item['tag'] == False and item['labeler'] == '':
                tag = '未标注'
            elif item['tag'] == False and item['labeler'] != '':
                tag = '注意'
                if 'infomation' in item:
                    allinfo = item['infomation']
                    if allinfo == None:
                        allinfo = ':'
                    if '：' in allinfo:
                        abbr,detail = allinfo.split('：')
                    else:
                        abbr = allinfo
                        detail = allinfo
                else:
                    abbr = ""
                    detail = ""
            elif item['tag'] == True:
                tag = '已标注'
            else:
                tag = '错误'
           
            temp = {
                'id':item['id'],
                'order':item['order'],
                'intent':item['intent'],
                'slots':item['label'],
                'sender':item['sender'],
                'tag':tag
            }
            return temp
        result = []
        for taskname in tasklist:
            fil = {
                "task":taskname
            }
            data = self.label.find(fil,{"_id":0,"history":0})
            task_res = [get_newitem(item) for item in data]
            result.extend(task_res)
        return result



    def get_label_table(self,name,task):
        def get_newitem(item):
            abbr = ""
            detail = ""
            if 'discard' in item and item['discard'] == True:
                tag = '废弃'
            elif item['tag'] == False and item['labeler'] == '':
                tag = '未标注'
            elif item['tag'] == False and item['labeler'] != '':
                tag = '注意'
                if 'infomation' in item:
                    allinfo = item['infomation']
                    if allinfo == None:
                        allinfo = ':'
                    if '：' in allinfo:
                        abbr,detail = allinfo.split('：')
                    else:
                        abbr = allinfo
                        detail = allinfo
                else:
                    abbr = ""
                    detail = ""
            elif item['tag'] == True:
                tag = '已标注'
            else:
                tag = '错误'
           
            temp = {
                'id':item['id'],
                'order':item['order'],
                'tag':tag,
                'infomation':detail,
                'abbr':abbr
            }
            return temp
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
                    order = self.label.find(fil,{"_id":0,"intent":0,"label":0,"history":0})
                    result = [get_newitem(item) for item in order]
                    
                    return result
            return {'error':'no task'}
        elif lbr['rule'] == 'admin':
            fil = {
                "task":task
            }
            order = self.label.find(fil,{"_id":0,"label":0,"history":0})
            result = [get_newitem(item) for item in order]
            return result
        else:
            return {'error':'user error'}


    def delete_labelData(self,task,id):
        res = self.label.delete_one({"task":task,"id":id})
        return res

    def work_labelData(self,task,id,labeler,intent,sender,labelinfo,tag=True,infomation='',discard=False,fixed=''):
        access = self.check_task(labeler,task,id)
        if access == "access":
            if self.label.find_one({"task":task,"id":id}):
                # 对于是否修改原始order的两套逻辑
                if fixed == '':
                    upinfo = {
                        'labeler':labeler,
                        'label':labelinfo,
                        'tag':tag,
                        'sender':sender,
                        "intent":intent,
                        'discard':discard,
                    }
                else:
                    lbr = self.label.find_one({"task":task,"id":id})
                    order = lbr['order']
                    if 'history' in lbr :
                        history = lbr['history']
                        history.append(order)
                    else:
                        history = [order]
                    upinfo = {
                        'labeler':labeler,
                        'label':[],
                        'tag':tag,
                        'sender':sender,
                        "intent":intent,
                        'order':fixed,
                        'history':history,
                    }
                if infomation != "":
                    upinfo['infomation'] = infomation
                
                res = self.label.update_one({"id":id,'task':task},{"$set":upinfo})
                res = self.label.find_one({"id":id,'task':task})
                res = str(res['id']) + 'update success'
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

    #---------------------admin--------------------------
    def admin_add_task(self,name,task,labeler):
        admin = self.labeler.find_one({'name':name})
        if admin and admin['rule'] == 'admin':
            tasklist = admin['task']
            tasklist.append(task)
            self.labeler.update_one({'name':name},{"$set":{'task':tasklist}})
            labelerlist = admin['labelerlist']
            labelerlist[task] = labeler
            self.labeler.update_one({'name':name},{"$set":{'labelerlist':labelerlist}})
            return True
        else:
            return False
    

    def admin_check_task(self,name):
        admin = self.labeler.find_one({'name':name})
        if admin and admin['rule'] == 'admin':
            result = []
            tasklist = admin['task']
            labelerlist = admin['labelerlist']
            for task in tasklist:
                labelers = labelerlist[task]
                all_nums = self.label.count_documents({'task':task})
                labeled_num = self.label.count_documents({'task':task,'tag':True})
                person_count = []
                for labeler in labelers:
                    lbr = self.labeler.find_one({'name':labeler})
                    tasklist = lbr['task']
                    for item in tasklist:
                        if item['taskname'] == task:
                            start,end = item['ids']
                            break
                    person_allnum = self.label.count_documents({'task':task,'id':{"$gte":start,"$lte":end}})
                    person_labelnum = self.label.count_documents({'task':task,'id':{"$gte":start,"$lte":end},'tag':True})
                    temp = {
                        'labeler':labeler,
                        'ids':[start,end],
                        'all_num':person_allnum,
                        'labeled_num':person_labelnum,
                    }
                    person_count.append(temp)
                tasktemp ={
                    'taskname':task,
                    'all_order':all_nums,
                    'labeled_num':labeled_num,
                    'labeler_situation':person_count
                }
                result.append(tasktemp)
            return result   
        else:
            return {'error':'access refused'}

    def admin_output(self,name,task):
        admin = self.labeler.find_one({'name':name})
        if admin and admin['rule'] == 'admin':
            pass
        else:
            return {'error':'access refused'}

    def admin_task_clear(self,name,task):
        admin = self.labeler.find_one({'name':name})
        if admin and admin['rule'] == 'admin':
            #删掉admin中的任务信息
            tasklist = admin['task']
            tasklist.remove(task)
            self.labeler.update_one({'name':name},{"$set":{'task':tasklist}})
            labelerlist = admin['labelerlist']
            labelers = labelerlist.pop(task)
            self.labeler.update_one({'name':name},{"$set":{'labelerlist':labelerlist}})
            #删掉labeler中的任务信息
            for labeler in labelers:
                lbr = self.labeler.find_one({'name':labeler})
                tasklist = lbr['task']
                for taskitem in tasklist:
                    if taskitem['taskname'] == task:
                        break
                tasklist.remove(taskitem)
                self.labeler.update_one({'name':labeler},{"$set":{'task':tasklist}})
            #删掉label中的任务信息
            self.label.delete_many({'task':task})
            return True
        else:
            return False

    def admin_task_insert(self):
        pass

    def admin_manage_task(self):
        pass

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
    # db.manage_task('task/minhang.xlsx',taskname='test2',labelers=['fei','wang'])
    # res = db._set_labeler_password('wang')
    # res = db.check_labeler('wang','123456')
    # res = db.add_labeler('admin',password='admin',rule='admin')
    # res = db.admin_add_task('fei','test2',['fei','wang'])
    # res = db.admin_check_task('admin')
    
    # name = 'atc10'
    # password = '367788'
    # res = db.add_labeler(name=name,password=password)
    # res = db.labeler.delete_one({'name':'atc01'})
    # res = db.check_labeler('atc01','123456')
    # res = db.admin_task_clear('admin','test2')
    res = db.manage_task('task/task21.xlsx','task21',['atc06'])
    # res = db.admin_task_clear('admin','task06')
    print(res)