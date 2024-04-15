import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import xlsxwriter

load_dotenv("SLA DP envionment file.env")

'''
Cleaner Module. 
It has 2 class
1. Invoice Cleaner used to Process the Invoice data frames 
2. SLA Cleaner used to process the SLA data Frames
'''

class pre_processor:
    def __init__(self, files):
        self.files = files
    def modified_dfs(self):
        files_holder = []
        for file in self.files:    
            xls = pd.ExcelFile(file)
            for sheet in xls.sheet_names:
                if 'sla' in sheet.lower():
                    df_sla = pd.read_excel(file, sheet_name= sheet)
                elif 'invoice' in sheet.lower():
                    df_invoice = pd.read_excel(file, sheet_name= sheet)
            df_invoice = pd.merge(df_invoice, df_sla[['Link ID', 'SLA Date']], on='Link ID', how = 'left')

            """
            
            Merging the sheet into an excel workbook
            """

            with pd.ExcelWriter(file, engine='xlsxwriter', mode='w') as writer:
                df_sla.to_excel(writer, sheet_name='SLA', index=False)
                df_invoice.to_excel(writer,sheet_name='invoice', index=False)
            files_holder.append(file)
        return files_holder

class InvoiceCleaner:
    def __init__(self, df,sp):
        self.df = df
        self.sp = sp

    def drop_null_rows(self):
        self.df.dropna(how='all', inplace=True)

    def drop_total_row(self):
        if "No." in self.df.columns:
            self.df.drop(self.df[self.df["No."] == "Total"].index, inplace=True)

    def pick_invoice_column_name(self):
        for substring in ['invoice no', 'ref']:
            matching_columns = [col for col in self.df.columns if substring.lower() in col.lower()]
            if matching_columns:
                return matching_columns[0]

    def QRC_Finder(self):
        max_value = None
        max_col = None
        for substring in ['QRC', 'Total', 'Amount']:
            fields = [col for col in self.df.columns if substring.lower() in col.lower()]
            for field in fields:
                if field in self.df.columns and not self.df[field].isnull().all():
                    first_row_value = self.df.at[1, field]
                    if max_value is None or first_row_value > max_value:
                        max_value = first_row_value
                        max_col = field
        return max_col
    


    def transform_data(self):
        self.drop_null_rows()
        self.drop_total_row()

        columns_wanted = {
            "Date": "Invoice Date",
            "ID": "Link ID",
            "SLA Date": "SLA Date",
            "Period": "Invoice Period",
            "Description": "Invoice Description",
            "Reference/No": self.pick_invoice_column_name(),
            "QRC": self.QRC_Finder()
        }

        self.df = self.df.loc[:, [value for value in columns_wanted.values()]]

        new_invoice_col_names = {
            "Link ID": "Link_ID",
            self.pick_invoice_column_name(): "Invoice_Reference",
            "Invoice Date": "Invoice_Date",
            "SLA Date": "SLA_Date",
            "Invoice Period": "Invoice_Period",
            "Invoice Description": "Invoice_Description",            
            self.QRC_Finder(): "Total_QRC"
        }
        self.df = self.df.rename(columns=new_invoice_col_names)

        # Adjust 'Link ID' based on its current type
        if 'Link_ID' in self.df.columns:
            if self.df['Link_ID'].dtype == 'float':
                self.df['Link_ID'] = self.df['Link_ID'].astype('int').astype('str')
            elif self.df['Link_ID'].dtype == 'int':
                self.df['Link_ID'] = self.df['Link_ID'].astype('str')

        # Convert 'Invoice Date' to datetime
        if 'Invoice_Date' in self.df.columns:
            self.df['Invoice_Date'] = pd.to_datetime(self.df['Invoice_Date'])


        # Harmoninzing the Invoice Peiods
        if 'Invoice_Period' in self.df.columns:
            Invoice_period_replacer = {
                '1st Oct to 31st Dec 2023' : '01-Oct-2023 to 31-Dec-2023',
                '01-Oct-2023 to 31-Dec-2023' : '01-Oct-2023 to 31-Dec-2023'
            }
            self.df['Invoice_Period'] = self.df['Invoice_Period'].replace(Invoice_period_replacer)

        "Adding UI Cols, First we will Created a SLA ID Col"

        #Extracting unique values from the date col and sort them
        unique_dates = sorted(self.df['SLA_Date'].unique())

        #creating a mapping of date to rank
        date_to_rank = {date: f'{i:02d}' for i,date in enumerate(unique_dates)}

        #add a new col called rank based on the mapping
        self.df['rank'] =  self.df['SLA_Date'].map(date_to_rank)

        #Adding the SLA ID Col
        self.df['SLA_ID'] = self.df.apply(lambda row: f"{row['SLA_Date'].year}-{row['SLA_Date'].month}-{self.sp}{row['rank']}", axis=1)

        #Unique_Link_ID a combination of SLA_ID And Link_ID
        self.df['Unique_Link_ID'] = self.df['SLA_ID'] + '_' + self.df['Link_ID'].astype(str)

        return self.df


    """
    def new_col_creator(self):
        self.transform_data()

    def invoice_duplicate_handler(self):
        self.transform_data()
        #Handling the Duplicates Link_IDs
            
        # Identify duplicate Link_IDs
        duplicate_link_ids = self.df['Link_ID'].value_counts()[lambda x:x >1].index

        #Checking if there are duplicate Link IDS
        if len(duplicate_link_ids) > 0:        
        
            # Iterate over each duplicate Link ID
            for link_id in duplicate_link_ids:

             # Filter the dataframe for all occurrences of the current Link ID
                duplicates = self.df[self.df['Link_ID'] == link_id]
            
                # Find the index of the row with the highest Total_QRC
                max_qrc_index = duplicates['Total_QRC'].idxmax()
            
                # Drop all other duplicates except for the one with the highest Total_QRC
                self.df = self.df.drop(duplicates.index.difference([max_qrc_index]))
        return self.df

    """
    def clean_data(self):
        self.df = self.transform_data()
        return self.df



class SLACleaner:
    def __init__(self, df,sp):
        self.df = df
        self.sp = sp
        

    def drop_null_rows(self):
        self.df.dropna(how='all', inplace=True)

    def drop_total_row(self):
        if "No." in self.df.columns:
            self.df.drop(self.df[self.df["No."] == "Total"].index, inplace=True)

    def capacity_picker(self):
        for pat in ['Capacity']:
            matching_cols = [col for col in self.df.columns if pat.lower() in col.lower()]
            if matching_cols:
                return matching_cols[0]

    def comments_picker(self):
        for pat in ['comment']:
            matching_cols = [col for col in self.df.columns if pat.lower() in col.lower()]
            if matching_cols:
                return matching_cols[0]

    def site_desc_picker(self):
        for pat in ['site', 'description']:
            matching_cols = [col for col in self.df.columns if pat.lower() in col.lower()]
            if matching_cols:
                return matching_cols[0]

    def MRC_picker(self):
        for pat in ['month', 'MRC']:
            matching_cols = [col for col in self.df.columns if pat.lower() in col.lower()]
            if matching_cols:
                return matching_cols[0]

    def clean_data(self):
        self.drop_null_rows()
        self.drop_total_row()

        SLA_Cols = {
            'Link ID': 'Link ID',
            'SLA Dates': 'SLA Date',
            'Last Mile': 'Last Mile',
            'Capacity': self.capacity_picker(),
            'Sites/Location': self.site_desc_picker(),
            'MRC': self.MRC_picker(),
            'Comments': self.comments_picker()
        }

        self.df = self.df.loc[:, [col for col in SLA_Cols.values()]]

        # Changing the link id to str depending on the current dtype
        if 'Link ID' in self.df.columns:
            if self.df['Link ID'].dtype == 'float':
                self.df['Link ID'] = self.df['Link ID'].astype('int').astype('str')
            elif self.df['Link ID'].dtype == 'int':
                self.df['Link ID'] = self.df['Link ID'].astype('str')
            else:
                self.df['Link ID'] = self.df['Link ID'].astype('str')

        # Convert 'SLA Date' to datetime
               
        if 'SLA Date' in self.df.columns:
            self.df['SLA Date'] = pd.to_datetime(self.df['SLA Date'])           
            

        # Removing any trailling or leading spcae in Lastmile Col
        if 'Last Mile' in self.df.columns:
            self.df['Last Mile'] = self.df['Last Mile'].str.strip()

        # Capacity
        if self.capacity_picker() in self.df.columns and self.df[self.capacity_picker()].dtype == 'object':
            self.df[self.capacity_picker()] = self.df[self.capacity_picker()].str.split(' ', expand=True)[0].astype(
                'int')

        # Calculating the Quarterly Recurring Charges
        if self.MRC_picker() in self.df.columns:
            self.excise_duty = float(os.getenv('excise_duty'))
            self.VAT = float(os.getenv('VAT'))
            self.df['QRC'] = np.round(np.where(
                self.df['Last Mile'].str.lower() == 'internet', # Internet taxation which includes both excise and VAT
                ((self.df[self.MRC_picker()] * (1+self.excise_duty)) * (1+self.VAT)*3),                
                # Multiprotocol label Switching (MPLS) which includes only VAT
                np.multiply(self.df[self.MRC_picker()] * (1+self.VAT),3)              

            ), 2)
            '''
            if self.df['Last Mile'].lower() == 'Internet':
                self.df['QRC'] = np.multiply((self.df[self.MRC_picker()] * (1+self.excise_duty)) * (1+self.VAT),3)
            elif self.df['Last Mile'].str.lower() == 'mpls':
                self.df['QRC'] = np.multiply(self.df[self.MRC_picker()] * (1+self.VAT),3)
            '''

        # Renaming the Columns
        SLA_New_Names = {
            'Link ID': 'Link_ID',
            'SLA Date': 'SLA_Date',
            'Last Mile': 'Last_Mile',
            'QRC': 'QRC_Incl',
            self.site_desc_picker(): 'Location',
            self.capacity_picker(): 'Capacity_in_Mbps',
            self.MRC_picker(): 'MRC_Excl',
            self.comments_picker(): 'SLM_Comments'
        }
        self.df = self.df.rename(columns=SLA_New_Names)


        "Adding UI Cols, First we will Created a SLA ID Col"

        #Extracting unique values from the date col and sort them
        unique_dates = sorted(self.df['SLA_Date'].unique())

        #creating a mapping of date to rank
        date_to_rank = {date: f'{i:02d}' for i,date in enumerate(unique_dates)}

        #add a new col called rank based on the mapping
        self.df['rank'] =  self.df['SLA_Date'].map(date_to_rank)

        #Adding the SLA ID Col
        self.df['SLA_ID'] = self.df.apply(lambda row: f"{row['SLA_Date'].year}-{row['SLA_Date'].month}-{self.sp}{row['rank']}", axis=1)

        #Unique_Link_ID a combination of SLA_ID And Link_ID
        self.df['Unique_Link_ID'] = self.df['SLA_ID'] + '_' + self.df['Link_ID'].astype(str)

        return self.df