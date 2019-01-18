import csv
import logging

from matplotlib import pyplot as plt

from utils.logging import setup_logging,get_module_by_name



logger = setup_logging([get_module_by_name(__name__)],
                       level=logging.INFO, 
                       module_logger_to_return = get_module_by_name(__name__))

if __name__ == '__main__':
    logger.info('start.')
    csv_fn = '../SOResults/tag_vs_postcount.csv'
    with open(csv_fn, newline='') as csvfile:
        next(csvfile)   # skip header
        csv_reader = csv.reader(csvfile, delimiter=',')
        counts = [int(count) for _,_,count in csv_reader]
    logger.info('load tags\' post counts from ' + csv_fn)
    plt.loglog(range(0, len(counts)), counts)
    plt.xlabel('Tag Rank (2008-07-31 to 2017-03-13)')
    plt.ylabel('Post Counts')
    plt.show()

