"""
Pre-processing Pipeline
"""

#########################################################################################
# Albeado Data Scientist Technical Exam		       										#
# This code pre-preocess the raw Enron email data. Clean,transform the timestamp,entity #
#                                                                                       #
# Author: Dawei(David) Wang	                                                            #
#                                                                                       #
#                                    Version 1.0                                        #
#########################################################################################


import numpy as np
import pandas as pd
import datetime
from datetime import datetime as dt

INPUT_DIR = '../data/enron-event-history-all.csv'
OUTPUT_DIR = '../data/cleaned1.csv'

############################################################################
##################################Helper Functions##########################
############################################################################

## Execution time helper function

class timeit():
	from datetime import datetime
	def __enter__(self):
		self.tic = self.datetime.now()
	def __exit__(self, *args, **kwargs):
		print('runtime: {}'.format(self.datetime.now() - self.tic))

		
## Split list in columns to multiple rows

def split_list_in_cols_to_rows(df, lst_cols, fill_value=''):
	# make sure `lst_cols` is a list
	if lst_cols and not isinstance(lst_cols, list):
		lst_cols = [lst_cols]
	# all columns except `lst_cols`
	idx_cols = df.columns.difference(lst_cols)

	# calculate lengths of lists
	lens = df[lst_cols[0]].str.len()

	if (lens > 0).all():
		# ALL lists in cells aren't empty
		return pd.DataFrame({
			col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
			for col in idx_cols
		}).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
		  .loc[:, df.columns]
	else:
		# at least one list in cells is empty
		return pd.DataFrame({
			col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
			for col in idx_cols
		}).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
		  .append(df.loc[lens==0, idx_cols]).fillna(fill_value) \
		  .loc[:, df.columns]

def preprocessing(input_dir,output_dir):

		"""
		This function pre-preocess the raw Enron email data.  
        Clean,transform the timestamp,entity.
        Split multiple recipients

        Parameters
        ----------
        input_dir : str
            The file path of input csv file
        output_dir : str
            The file path of output csv file
        
        Returns
        --------
        .csv file
            The cleaned dataframe saved as .csv file in output_dir
        """

	All_df = pd.read_csv(input_dir,
                     names = ['time','message','sender','recipients','topic','mode'])

	## Drop columns
	All_df.drop(['topic','mode','message'],axis=1,inplace=True)

	## Dropna
	All_df.dropna(how='any',inplace=True)

	## Transform unix time to datetime
	All_df['time'] = pd.to_datetime(All_df['time'],unit='ms')

	## Replace blanc/outlook
	All_df.sender.replace(['notes', 'blank','outlook'], 'unknown', inplace=True)

	## Split multiple recipients
	All_df = split_list_in_cols_to_rows(All_df.assign(recipients=All_df.recipients.str.split('|')), 'recipients')

	## Extract name from email address
	All_df.recipients = All_df.recipients.apply(lambda x: x.split("@")[0])
	All_df.sender = All_df.sender.apply(lambda x: x.split("@")[0])

	## Save it to csv file
	All_df.to_csv(output_dir,index = False)


def main():
	preprocessing(INPUT_DIR,OUTPUT_DIR)

if __name__ == '__main__':
	main()