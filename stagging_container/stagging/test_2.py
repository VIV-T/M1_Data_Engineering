
import time
import logging
import os

def test() :
    with open(file=f"{os.getcwd()}/project_data//output_2.txt", mode='w', encoding='utf-8') as f :
        f.write('writting test from the container number 2\n')
        f.write(os.getcwd())
        logging.info("file written properly")

test()
time.sleep(10)