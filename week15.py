from gevent import monkey
monkey.patch_all()
import requests,time,os,urllib.request,gevent
from bs4 import BeautifulSoup
from queue import Queue
from playsound import playsound
from pymongo import MongoClient

def GetPage(url,headers):
    '''
    获取页数
    '''
    response=requests.get(url=url,headers=headers)
    soup=BeautifulSoup(response.text,'lxml')
    return int(soup.select('a[class="zpgi"]')[-1].get_text())

def DownloadImage(url,dir_path):
    '''
    下载图片
    '''
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
    name=url.split('/')[-1]
    img_path=dir_path+'/'+name
    try:
        urllib.request.urlretrieve(url,filename=img_path)
        urllib.request.urlcleanup()
    except:
        return 'error'
    return img_path

def Producer(q:Queue,url,headers):
    '''
    生产者
    '''
    response=requests.get(url=url,headers=headers)
    soup=BeautifulSoup(response.text,'lxml')
    soup_list=soup.select('a[class="tit f-thide s-fc0"]')
    #print(soup_list)
    href_list=[]
    for s in soup_list:
        href_list.append('https://music.163.com'+str(s['href']))
    q.put(href_list)


def Consumer(q:Queue,headers,dir_path):
    '''
    消费者
    '''
    head=['id','title','image','author_id','author','description','count','play','add','share','comment']
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
    urls=q.get()
    if urls!=None:
        for url in urls:
            response=requests.get(url=url,headers=headers)
            soup=BeautifulSoup(response.text,'lxml')
            id=url.split('id=')[-1]#歌单id
            title=soup.select('.tit')[0].get_text()[1:]#歌单标题
            image_url=soup.select('img[class="j-img"]')[0]['data-src']#封面url
            image=DownloadImage(image_url,dir_path+'/images')#下载图片
            author_id=soup.select('a[class="s-fc7"]')[0]['href'].split('id=')[-1]#作者id
            author=soup.select('a[class="s-fc7"]')[0].get_text()#作者名
            description=soup.select('p')[1].get_text()#简介
            count=soup.select('span[id="playlist-track-count"]')[0].get_text()#歌曲数
            play=soup.select('strong[id="play-count"]')[0].get_text()#播放次数
            add=soup.select('a[class="u-btni u-btni-fav"]')[0]['data-count']#收藏数
            share=soup.select('a[class="u-btni u-btni-share"]')[0]['data-count']#分享数
            comment=soup.select('span[id="cnt_comment_count"]')[0].get_text()#评论数
            #按行写入
            with MongoClient('localhost',27017) as client:
                database=client['test_database']
                collection=database['test_collection']
                collection.insert_one(dict(zip(head,[id,title,image,author_id,author,description,count,play,add,share,comment])))
                
def CoroProducer(q:Queue,url):
    '''
    协程执行Producer
    '''
    tasks=[]
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.35'}
    N=GetPage(url,headers)#获取页数
    #N=3
    print('This website has {} pages to crawl'.format(N))
    urls=[]
    for i in range(N):
        urls.append(url[:-1]+str(i*35))
    for url in urls:
        task=gevent.spawn(Producer,q,url,headers)
        tasks.append(task)
    gevent.joinall(tasks)
    return N

def CoroConsumer(q:Queue,N,dir_path):
    '''
    协程执行Consumer
    '''
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.35'}
    tasks=[]
    for i in range(N):
        task=gevent.spawn(Consumer,q,headers,dir_path)
        tasks.append(task)
    gevent.joinall(tasks)

if __name__=='__main__':
    dir_path='D:/Project/Python/week15/result'
    url='https://music.163.com/discover/playlist/?order=hot&cat=%E6%B0%91%E8%B0%A3&limit=35&offset=0'
    q=Queue()
    t_start=time.time()
    N=CoroProducer(q,url)#协程执行Producer
    print('Producers have been executed, all tasks cost {}s'.format(time.time()-t_start))
    CoroConsumer(q,N,dir_path)#协程执行Consumer
    print('Consumers have been executed, all tasks cost {}s'.format(time.time()-t_start))
    playsound('D:\Project\Python\week15\over.mp3')#程序运行完成的声音提醒