import requests
import datetime
import base64
import hmac
import sha
import os
import sys
import re
import time
import logging
import json
from multiprocessing.dummy import Pool as ThreadPool
sy = sys.argv[1]
sy1 = sys.argv[2]
choice = sys.argv[3]

root_url= "http://faceall-yunjingwang.oss-cn-beijing.aliyuncs.com/?prefix="+sy+"&max-keys=5&marker="
download_url= "http://faceall-yunjingwang.oss-cn-beijing.aliyuncs.com/"
save_dir = "/local/data/api-image-backup/"

debug_dir1 = '/local/data/api-data/logs-9991/'
debug_dir2 = '/local/data/api-data/logs-9992/'

root_dir = os.path.join("/local/data/api-image-backup",sy)
filename = os.listdir(root_dir)
#regex
key_patt = re.compile('<Key>([\s\S]+?)</Key>')
marker_patt = re.compile('<NextMarker>([\s\S]+?)</NextMarker>')
#time
st = time.time()
#log
logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

#get header
def get_header(method, s):
    GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'
    ctime = datetime.datetime.utcnow().strftime(GMT_FORMAT)
    s=str(method)+"\n\n\n"+ctime+"\n/faceall-yunjingwang/"+s
    h = hmac.new("Kk1gHdq9GWoxRvjMCwm8FQCUtisTkr", s,sha)
    Signature = base64.b64encode(h.digest())
    headers = {'Date':ctime,'Authorization':'OSS LTAIPAfvS4sCuwBl:'+Signature}
    return headers
#get_log_message
def get_dict():
    result = {}
    with open(debug_dir1+'debug.log-'+sy1) as f:
        for i in f.readlines():
            m = re.search('porn',i.replace('\n',''))
            if m is not None:
                temp = i.split('#')
                imagename = temp[5].split('/')[-1]
                porn = json.loads(temp[6])['porn']
                result[imagename] = 'prob_'+str(porn['probability'])+'_id_'+str(porn['id'])+'_'
    with open(debug_dir2+'debug.log-'+sy1) as f:
        for j in f.readlines():
            m = re.search('porn',j.replace('\n',''))
            if m is not None:
                temp = j.split('#')
                imagename = temp[5].split('/')[-1]
                porn = json.loads(temp[6])['porn']
                result[imagename] = 'prob_'+str(porn['probability'])+'_id_'+str(porn['id'])+'_'
    return result       
#get list
def get_list():
    result = []
    marker = ['']
    try:
        while True:
            url = root_url + marker[0]
            r=requests.get(url,headers=get_header("GET", ''))
            logger.info('{} {}'.format(r.status_code, time.time()-st))
            key = key_patt.findall(r.text)
            marker = marker_patt.findall(r.text)
            for i in key:
                result.append(i)
            if len(marker) == 0:
                break           
    except Exception, e:
        logger.info(e)
    return result

#get_image_info
def get_image_info(filename):
    url = download_url + filename + "?x-oss-process=image/info"
    try:  
        r=requests.get(url, headers=get_header("GET", str(filename)+"?x-oss-process=image/info"))
        logger.info(filename+"#"+str(r.text).replace("\n", ""))
    except Exception, e:
        logger.info(e)

#delete
def delete(filename):
    url = download_url + filename
    try:
        r = requests.delete(url, headers = get_header('DELETE', str(filename)))
    except Exception,e:
        logger.info(e)
    logger.info(r.content)
    logger.info(filename+" delete success!")
#upload
def upload(i):
    src_name = os.path.join(root_dir,i)
    des_name = src_name.split(".")[0]+"_resize.jpg"
    os.system("convert "+ src_name +" -resize '1024x1024>' "+des_name)
    try:
        with open(des_name) as f:
            if str(i) in image_dict:      
                filename = sy+"/"+image_dict[str(i)]+str(i)
            else:
                filename = sy+"/"+str(i)
            try:
                r=requests.put(download_url+filename, data=f.read(), headers=get_header("PUT", str(filename)))
                logger.info('{} {}'.format(r.status_code, time.time()-st))
            except Exception, e:
                logger.info(e)
            finally:
                os.remove(src_name)
                os.remove(des_name)
    except:pass

#download
def download(filename):
    url = download_url + filename
    try:  
        r=requests.get(url,headers=get_header('GET', str(filename)))
    except Exception, e:
        logger.info(e)
    with open(save_dir+filename ,'w+') as f:
        f.write(r.content)

if __name__=="__main__":   
    if choice == "download":
        if not os.path.exists(sy):       
            os.system('mkdir '+sy)
        file_list = get_list()
        pool = ThreadPool(processes=20)
        pool.map(download, file_list) 
        pool.close()
        pool.join()

    elif choice == "delete":
        file_list = get_list()
        pool = ThreadPool(processes=20)
        pool.map(delete, file_list) 
        pool.close()
        pool.join()
    elif choice == "get_image_info":
        file_list = get_list()
        pool = ThreadPool(processes=20)
        pool.map(get_image_info, file_list) 
        pool.close()
        pool.join()
    elif choice == "upload":
        image_dict = get_dict()
        pool = ThreadPool(processes=20)
        pool.map(upload, filename) 
        pool.close()
        pool.join()
        os.system("rm -r "+'/local/data/api-image-backup/'+sy)

