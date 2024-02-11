
from function.crawler      import crawl_and_save
from function.comparison   import compare_to_save
from function.data_dispose import dispose

def Main():
    crawl_and_save()
    compare_to_save()
    dispose()

if __name__ == '__main__':
    Main()

