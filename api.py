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
    }
    '''
    data = request.get_json()
    
    name = data['labeler']
    task = data['task']
    mode = data['cut_mode']
    num = data['num']
    json_res = []
    result = ldb.get_unlabeledData(name,task,num)
    if isinstance(result,str):
        return {"error":result}
    else:
        for item in result:
            order = item['order']
            if mode == 1:
                wordlist = cuter.cut_order(order)
            else:
                wordlist = cuter.cut_order_word(order)
            index = word2index(wordlist)
            candidates = []
            for w,i in zip(wordlist,index):
                temp = {
                    "word":w,
                    "ids":i
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
    data = request.get_json()
    task = data['task']
    id = data['id']
    labeler = data['labeler']
    intent = data['intent']
    sender = data['sender']
    candidate = data['candidates']
    res = ldb.work_labelData(task=task,id=id,labeler=labeler,intent=intent,sender=sender,labelinfo=candidate)
    return res

if __name__=='__main__':
    app.run(host='0.0.0.0',port=8093)