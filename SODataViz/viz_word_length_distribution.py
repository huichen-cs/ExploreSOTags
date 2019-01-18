import matplotlib.pyplot as plt
from statistics import median

wlen_fn = '../SOResults/word_length.csv'
with open(wlen_fn, 'r') as f:
    wlen = [int(l) for l in f]
plt.hist(wlen, 50, facecolor='gray', alpha=0.75)
plt.text(20000, 10**6
         , '     Min Length: ' + str(min(wlen)) + '\n'
         + 'Median Length: ' + str(int(median(wlen))) + '\n'
         + '     Max Length: ' + str(max(wlen)))
plt.yscale('log', nonposy='clip')
plt.xlabel('Word Length')
plt.ylabel('Occurrences')
plt.show()