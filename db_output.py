import os
import json
from db_control import DB_Contoller
import pandas as pd
import config as cfg

def data2excel(data,save_path,save_name):
    id = []
    order = []
    intent = []
    tag = []
    for item in data:
        if item['tag'] == '已标注' or item['tag'] == '存疑':
            id.append(item['id'])
            order.append(item['order'])
            temp_intent_str = '/'.join(item['intent'])
            intent.append(temp_intent_str)
            if item['tag'] == '存疑':
                tag.append('存疑')
            else:
                tag.append('')
    output = pd.DataFrame({
        'id':id,
        '空管指令':order,
        '意图':intent,
        '备注':tag
    })
    output.to_excel(os.path.join(save_path,save_name),sheet_name='Sheet1',index=False)

def saveJson(data,filePath):
    json_dicts = json.dumps(data,indent=4,ensure_ascii=False)
    with open(filePath,'w+',encoding='utf8') as fp:
        fp.write(json_dicts)

def data2json(data,save_path,save_name):
    res = []
    for item in data:
        if item['tag'] != '存疑':
            res.append(item)
    saveJson(res,os.path.join(save_path,save_name))


if __name__=="__main__":
    db = DB_Contoller(cfg)
    tasklist = [
        "task01",
        "task06",
        "task05",
        "task04",
        "task03",
        "task02",
        "task07",
        "task08",
        "task09",
        "task10",
        "task11",
        "task12",
        "task13",
        "task14",
        "task15",
        "task16",
        "task17",
        "task18",
        "task19",
        "task20",
        "task21"
    ]
    data = db._get_data(tasklist=tasklist)
    save_path = 'output'
    save_name = 'output_23_3_22.json'
    # data2excel(data,save_path,save_name)
    data2json(data,save_path,save_name)

    numss = 0
    in_list = []
    for m in data:
        str = ''
        length = 0
        if m["tag"] == "已标注":
            numss += 1
            for n in m["slots"]:
                if numss == 214:
                    print(n["word"])
                length += 1
                str += n["word"] + ' '
            in_list.append(str)
            if numss == 214:
                print(str)
                print(length)
    # file_path = os.path.join(save_path, 'seq.in')
    # with open(file_path, 'w', encoding='utf-8') as f:
    #     for i in in_list:
    #         f.write(i+'\n')

    # nums = 0
    # out_list = []
    # for m in data:
    #     str = ""
    #     before = ""
    #     length = 0
    #     if m["tag"] == "已标注":
    #         nums += 1
    #         for n in m["slots"]:
    #             length += 1
    #             if n["BIO"] == "O":
    #                 str += n["BIO"] + " "
    #             elif n["BIO"] == before:
    #                 str += "I-" + n["BIO"] + " "
    #             else:
    #                 str += "B-" + n["BIO"] + " "
    #                 before = n["BIO"]
    #         if nums == 214:
    #             print(str)
    #             print(length)
    #         out_list.append(str)
    # file_path = os.path.join(save_path, 'seq.out')
    # with open(file_path, 'w', encoding='utf-8') as f:
    #     for i in out_list:
    #         f.write(i+'\n')
    
    # label_list = []
    # for m in data:
    #     str = ''
    #     split = ''
    #     if m["tag"] == "已标注":
    #         for n in m["intent"]:
    #             str += split + n
    #             split = '#'
    #         label_list.append(str)
    # file_path = os.path.join(save_path, 'label')
    # with open(file_path, 'w', encoding='utf-8') as f:
    #     for i in label_list:
    #         f.write(i+'\n')


    sender_list = []
    for m in data:
        str = ''
        if m["tag"] == "已标注":
            str = m["sender"]
            sender_list.append(str)
    file_path = os.path.join(save_path, 'sender')
    with open(file_path, 'w', encoding='utf-8') as f:
        for i in sender_list:
            f.write(i+'\n')