import multiprocessing as mp
#from selenium import webdriver
from selenium.webdriver.common.by import By
import gspread
import time
import telebot
import threading
import random
from selenium import webdriver

#bot = telebot.TeleBot('6863128147:AAGgI6b2nlG2oI_mZuhDnUfJVtvsvhrDIFU') #--scum 
#bot = telebot.TeleBot('6792642771:AAGnRxqnMG4Nlm-CqBPjjVsAY3ICIWTMUQo') #--on/off 
bot = telebot.TeleBot('6871067941:AAEekaqtZ3MjwDMrrTO0xUdrJg37BLcqReo')

def get_data_from_google_table(): #вытягивание всей инфы из таблицы
    gc = gspread.service_account(filename='D:/1_MY PROGS/PARSERS/проект кирилла/mytest-411319-99861ed21234.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1N-eSem5yEzAFLmCveUZNtnaYVJ_lPOKOvp3yVo4LK4M/edit#gid=0')
    worksheet = sh.sheet1
    list_of_exchangers_and_urls=[]
    for i in range(1,43,2):
        values_list = worksheet.col_values(i)
        list_of_exchangers_and_urls.append(values_list)
     
    # получение коллекции обменников; по итогу оказалось не нужно    
    exchangers=worksheet.row_values(2)
    result=[]
    for i in exchangers:
        y=i.split(", ")
        result.extend(y)
    result=set(result)
    result.discard("")
    # получение коллекции обменников; по итогу оказалось не нужно  
    return list_of_exchangers_and_urls, result #2 значение по итогу оказалось не нужно

def get_direction(url): #получение направление обмена из ссылки например - BTC-RUB
    direction=""
    for j in url[26:]:
        if j!=".":
            direction+=j
        else:
            break
    return direction

def parse_page(url): # вытягивание всех обменников представленных по ссылке
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    options1 = [{'proxy': {'http': 'http://vM2bF7:9fUlTFZMqQ@46.8.56.57:1050'}}, {'proxy': {'http': 'http://vM2bF7:9fUlTFZMqQ@45.151.145.246:1050'}}]

    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)                                          
        list_of_exchangers = driver.find_element(By.ID,"rates_block")
        list_of_exchangers = list_of_exchangers.find_element(By.ID,"content_table")
        list_of_exchangers = list_of_exchangers.find_element(By.TAG_NAME,"tbody")
        list_of_exchangers = list_of_exchangers.find_elements(By.TAG_NAME,"tr")
        #список объектов обменников на сайте получен
        names_of_exchangers_on_page = []
        for exchanger in list_of_exchangers:
            data = exchanger.find_element(By.CLASS_NAME,"bj")
            names_of_exchangers_on_page.append(data.text) #список названий обменников на сайте получен и добавлен
        
        return names_of_exchangers_on_page
        
    except Exception as ex:
        print(type(ex))
        f = open('erorr_log.txt', 'a')
        f.writelines(str(ex)+"\n\n\n")
        return [] 
   
def get_formated_data(element, result_structure_shared, lock):
    exchangers = element[1].split(", ")
    for url in element[2:]:
        names_of_exchangers_on_page = parse_page(url)
        if names_of_exchangers_on_page==[]: print("НИЧЕГО НЕ НАШЕЛ", url,end="\n\n")
        f = open('url_log.txt', 'a')
        f.writelines(url+" "+str(names_of_exchangers_on_page)+"\n\n\n")
        for exch in exchangers:
            if exch in names_of_exchangers_on_page:
                city = element[0]
                try:
                    for i in result_structure_shared:
                        if exch in i:
                            i[exch][city].append((url, get_direction(url), "+"))
                            break
                finally:
                    pass
            else:
                city = element[0]
                try:
                    for i in result_structure_shared:
                        if exch in i:
                            i[exch][city].append((url, get_direction(url), "-"))
                            break
                finally:                 
                    pass
        f.close()

def get_message_to_bot(result_list, exch=None):
    main_message=""
    for exchanger in result_list:
        first_key = list(exchanger.keys())[0]
        if exch==None or exch==first_key:
            message=""
            for key,value in exchanger.items():
                for key1,value1 in value.items():
                    absence=""
                    for element in value1:
                        if element[2]=="-":
                            absence+="\n"
                            absence+=key +" "+ element[0]
                    message+=absence
            main_message=message+"\n\n"

    return "Отсутствие-\n"+main_message

def create_result_structure(data):
    result_structure=list()
    for i in data:
        city=i[0]
        exchangers=i[1].split(", ")
        for j in exchangers:
            
            flag=-1
            for element in result_structure:
                if j in element:
                    flag=result_structure.index(element)
            
            if flag==-1:
                result_structure.append({j:{city:[]}})
            else:
                flag1=False
                for k in result_structure[flag][j]:
                    if k==city:
                        flag1=True
                if flag1==False:
                    result_structure[flag][j][city]=[]
    return result_structure

def convert_structure_to_shared(structure):
    manager = mp.Manager()
    result_structure_shared = manager.list()

    for item in structure:
        converted_item = manager.dict()

        for key, value in item.items():
            converted_value = manager.dict()

            for inner_key, inner_value in value.items():
                converted_inner_value = manager.list(inner_value)
                converted_value[inner_key] = converted_inner_value

            converted_item[key] = converted_value

        result_structure_shared.append(converted_item)

    return result_structure_shared                

def convert_structure_to_common(shared):
    common = []

    for item in shared:
        converted_item = {}

        for key, value in item.items():
            converted_value = {}

            for inner_key, inner_value in value.items():
                converted_inner_value = list(inner_value)
                converted_value[inner_key] = converted_inner_value

            converted_item[key] = converted_value

        common.append(converted_item)

    return common

def start_process():
    print('Starting', mp.current_process().name)

def main():
    main_data,y=get_data_from_google_table()     
    result_structure=create_result_structure(main_data)   
    result_structure_shared=convert_structure_to_shared(result_structure)
    
    manager = mp.Manager()
    lock = manager.Lock()
    pool_size = mp.cpu_count() * 2
    pool_size=pool_size//2
    pool = mp.Pool(
        processes=pool_size,
        initializer=start_process
    )
    for element in main_data:
            x=pool.apply_async(get_formated_data,(element, result_structure_shared, lock,))
            x.wait(1)
    pool.close()
    pool.join()
    
    result_structure=convert_structure_to_common(result_structure_shared)

    return result_structure
 
@bot.message_handler(content_types='text')
def starter(message):
    if message.text=="/start":
        bot.send_message(message.chat.id, "Выбери команду")
    
    elif message.text=="/report":
        try:
            for elem in ["City-Exchange","Pocket-Exchange","GoldObmen","1-Online","BTchange","MyBTC","4Money","24Zone", "BTchange", "ExCenter", "Money-Office"]:       
                x=get_message_to_bot(result_list=result,exch=elem)
                y=x.replace("\n","")
                if y!= "Отсутствие-":
                    bot.send_message(message.chat.id, x)  
        except Exception as ex:
            bot.send_message(message.chat.id, "Проверка еще не была произведена") 
            thread = threading.Thread(target=main_program, args=(message,))
            thread.start()
            thread.join()
            # while thread.is_alive():
            #     pass
            for elem in ["City-Exchange","Pocket-Exchange","GoldObmen","1-Online","BTchange","MyBTC","4Money","24Zone", "BTchange", "ExCenter", "Money-Office"]:       
                x=get_message_to_bot(result_list=result,exch=elem)
                if x :
                    bot.send_message(message.chat.id, x)             

def main_program(message):
    bot.send_message(message.chat.id, "Проверка началась")
    t1 = time.time()
    global result
    result = main()
    t2 = time.time()
    print("Прошло за ", t2 - t1)
    bot.send_message(message.chat.id, "Проверка завершена")
    message.text="/report"
    starter(message)
    time.sleep(60*10)
    return main_program(message)

if __name__ == '__main__':
    bot.polling(non_stop = True, interval = 0)
    

