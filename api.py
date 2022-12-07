from flask import Flask,request
from flask_cors import CORS
from db_control import DB_Contoller
from cut_mode import CutOrder,word2index
import config as cfg

app = Flask(__name__)
ldb = DB_Contoller(cfg)
cuter = CutOrder('dict/jiebaword.txt')
CORS(app, resources=r'/*')

@app.route('/data',methods=['POST'])
def get_data():
    '''
    jsondata = {
      "labeler":"abc",
      "task":"",
      "num": 1,
      "cut_mode": 1
      "id":1
    }
    '''
    data = request.get_json()
    
    name = data['labeler']
    task = data['task']
    mode = data['cut_mode']
    num = data['num']
    json_res = []
    if 'id' in data:
        id = data['id']
    else:
        id = -1
    result = ldb.get_unlabeledData(labeler=name,task=task,num=num,id=id)
    if isinstance(result,str):
        return {"error":result}
    else:
        for item in result:
            order = item['order']
            label = item['label']
            if mode == 0:
                if len(label) == 0:
                    wordlist = cuter.cut_order(order)
                    index = word2index(wordlist)
                    candidates = []
                    for w,i in zip(wordlist,index):
                        temp = {
                            "word":w,
                            "ids":i,
                            'BIO':'O'
                        }
                        candidates.append(temp)
                else:
                    candidates = label
            else:
                if mode == 1:
                    wordlist = cuter.cut_order(order)
                elif mode == 2:
                    wordlist = cuter.cut_order_word(order)
                else:
                    wordlist = cuter.cut_order_word(order)
                
                index = word2index(wordlist)
                candidates = []
                for w,i in zip(wordlist,index):
                    temp = {
                        "word":w,
                        "ids":i,
                        'BIO':'O'
                    }
                    candidates.append(temp)
                
            
            jsontemp = {
                "task":item['task'],
                "id":item['id'],
                "order":order,
                "intent":item['intent'],
                "sender":item['sender'],
                "candidates":candidates
            }
            json_res.append(jsontemp)
        return json_res


@app.route('/login',methods=['POST'])
def login():
    data = request.get_json()
    name = data['name']
    password = data['password']
    res = ldb.check_labeler(name,password)
    return {'info':res}

@app.route('/register',methods=['POST'])
def register():
    data = request.get_json()
    name = data['name']
    password = data['password']
    res = ldb.add_labeler(name,password)
    return {'result':res}

@app.route('/tasklist',methods=['POST'])
def tasklist():
    data = request.get_json()
    name = data['name']
    task = data['task']
    if task != '':
        
        res = ldb.get_label_table(name,task)
    else:
        res = ldb.get_tasklist(name)
    
    return res


@app.route('/labeled',methods=['POST'])
def update():
    '''
    data = {
        "task":"task",
        "id":1,
        "labeler":"fei",
        "intent":'intent',
        "sender":'sender',
        "candidates":[{
            "word":"1",
            "idx":[1,1],
            "BIO":"B-company"
        }]
    }
    '''
    try:
        data = request.get_json()
        task = data['task']
        id = data['id']
        labeler = data['labeler']
        intent = data['intent']
        sender = data['sender']
        tag = data['attention'] == False
        candidate = data['candidates']
        res = ldb.work_labelData(task=task,id=id,labeler=labeler,intent=intent,sender=sender,labelinfo=candidate,tag=tag)
        return res
    except:
        return {'error':Exception}
    

if __name__=='__main__':
    app.run(host='0.0.0.0',port=18080)