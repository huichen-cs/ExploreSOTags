import csv
import logging

from matplotlib import pyplot as pyplot

from utils.logging import setup_logging, get_module_by_name

logger = setup_logging([get_module_by_name(__name__)],
                       level=logging.INFO,
                       module_logger_to_return=get_module_by_name(__name__))


def setup_font_sizes():
    SMALL_SIZE = 14
    MEDIUM_SIZE = 16
    BIG_SIZE = 20
    pyplot.rc('font', size=SMALL_SIZE)          # controls default text sizes
    pyplot.rc('axes', titlesize=SMALL_SIZE)     # fontsize of the axes title
    pyplot.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
    pyplot.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
    pyplot.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
    pyplot.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
    pyplot.rc('figure', titlesize=BIG_SIZE)     # fontsize of the figure title


if __name__ == '__main__':
    logger.info('start.')
    csv_fn = '../SOResults/tag_vs_questioncount.csv'
    with open(csv_fn, newline='') as csvfile:
        next(csvfile)   # skip header
        csv_reader = csv.reader(csvfile, delimiter=',')
        counts = [(int(pcount), int(qcount)) for _, _, pcount, qcount in csv_reader]
    # pcounts = [c for c,_ in counts]
    qcounts = [c for _, c in counts]
    # dcounts = [(p-q)/q for p,q in counts]
    logger.info('load tags\' post counts from ' + csv_fn)
    setup_font_sizes()
    pyplot.loglog(range(0, len(qcounts)), qcounts, 'k-')
    # plt.loglog(range(0, len(dcounts)), dcounts, color='red')
    # pyplot.xlabel('Tag Rank (2008-07-31 to 2017-03-13)')
    pyplot.xlabel('Tag Rank')
    pyplot.ylabel('Question Count')
    pyplot.tight_layout()
    pyplot.savefig('../SOTagsPaper/figures/tag_vs_questioncount.pdf')
    pyplot.show()

