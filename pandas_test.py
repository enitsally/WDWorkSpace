import pandas as pd

# left = pd.DataFrame({'A': [1, 2, 3, 4, 1, 2, 100],
#                      'B': [1, 2, 3, 4, 2, 3, 2],
#                      'C': [1, 2, 3, 4, 3, 4, 3],
#                      'D': [1, 2, 3, 4, 4, 5, 100]})
# print left
# right = pd.DataFrame({'B': [1, 2, 2, 2],
#                       'C': [1, 2, 3, 3],
#                       'E': [10, 20, 20, 100],
#                       'F': [10, 40, 50, 100]})
# # print right
# # result_left = pd.merge(left, right, on=['B'], how='inner')
# # print result_left
#
# dict= {'A':'AA', 'B':'BB'}
# left = left.rename(columns=lambda x : dict[x] if x in dict else x)
# left.loc[:]['CCC'] = 0
# print left

# a = u'\xef\xbb\xbfDOE#'
# b = a.encode('ascii', 'ignore')
# print b

a = 'Abcd_ner Now(New) 2'
print a.lower()