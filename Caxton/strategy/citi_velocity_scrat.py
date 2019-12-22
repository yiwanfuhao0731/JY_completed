from datetime import datetime, date
from panormus.utils.chono import now

def chunks(lst, n):
    """
    Yield successive n-sized chunks from list

    :param list lst: list of things to chunk
    :param int n: chunk size
    :rtype: list
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__=='__main__':
    for k in chunks([i for i in range(20)],5):
        print (k)

    print (datetime(2000,1,1).date())
    print (now())