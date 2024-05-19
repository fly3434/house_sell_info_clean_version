from function.crawler      import crawl_and_save
from function.comparison   import compare_and_save
from function.data_filter  import filter
import sys

def Main():
    if (crawl_and_save() == False):
        print("crawl_and_save() Failed!")
        return False
    
    if (filter() == False):
        print("filter() Failed!")
        return False
    
    if (compare_and_save() == False): # save to XXX_new
        print("compare_and_save() Failed!")
        return False

if __name__ == '__main__':
    Main()

