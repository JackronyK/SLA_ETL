import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import xlsxwriter

load_dotenv("SLA DP envionment file.env")

'''
Cleaner Module. 
It has 3 class
1. Invoice Cleaner used to Process the Invoice data frames 
2. SLA Cleaner used to process the SLA data Frames
'''

class pre_processor:
    def __init__(self, files):
        self.files = files
    def modified_dfs(self):
        
        for file in self.files:               
            xls = pd.ExcelFile(file)
            df_invoice = pd.DataFrame() 
            df_sla = pd.DataFrame()
            for sheet in xls.sheet_names:
                if 'sla' in sheet.lower():
                    df_sla = pd.read_excel(file, sheet_name= sheet)
                elif 'invoice' in sheet.lower():
                    df_invoice = pd.read_excel(file, sheet_name= sheet)
            #Modifyinh the invoice df by adding SLA Date
            df_invoice = pd.merge(df_invoice, df_sla[['Link ID', 'SLA Date']], on='Link ID', how = 'left')

            """
            
            Merging the sheet into an excel workbook
            """
            file_path = os.path.splitext(file)[0]
            new_file_path = f'{file_path}pre_processed.xlsx'

            with pd.ExcelWriter(new_file_path, engine= 'xlsxwriter', mode='w') as writer:
                df_sla.to_excel(writer, sheet_name='SLA', index=False)
                df_invoice.to_excel(writer,sheet_name='invoice', index=False)

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

        ## Classifying the Invoice Period
        def Invoice_p_pro(prd):
            prd_col = pd.to_datetime(prd.split(' ')[0])
            year = prd_col.year
            month = prd_col.month

            if month >= 1 and month <= 3:
                quarter = 'Q3'
            elif month >= 4 and month <= 6:
                quarter = 'Q4'
            elif month >= 7 and month <= 9:
                quarter = 'Q1'
            elif month >= 10 and month <= 12:
                quarter = 'Q2'
            else:
                quarter = 'Unknown'
            
            # Financial Year
            if month >= 1 and month <= 6:
                fyr = f'Fyr {year-1}/{str(year)[-2:]}'
            elif month >= 7 and month <= 12:
                fyr = f'Fyr {year}/{str(year+1)[-2:]}'
            else:
                fyr = 'Unknown'

            return quarter, fyr
        
        self.df[['Invoice_Quarter', 'Invoice_Fyr']] = self.df['Invoice_Period'].apply(Invoice_p_pro).apply(pd.Series)
            
        "Adding UI Cols, First we will Created a SLA ID Col"

        #Extracting unique values from the date col and sort them
        unique_dates = sorted(self.df['SLA_Date'].unique())

        #creating a mapping of date to rank
        date_to_rank = {date: f'{i:02d}' for i,date in enumerate(unique_dates)}

        #add a new col called rank based on the mapping
        self.df['rank'] =  self.df['SLA_Date'].map(date_to_rank)

        #Adding the SLA ID Col
        self.df['SLA_ID'] = self.df.apply(lambda row: f"{row['SLA_Date'].year if pd.notnull(row['SLA_Date']) else '0000'}-{row['SLA_Date'].month if pd.notnull(row['SLA_Date']) else '00'}-{self.sp}{row['rank']}", axis=1)

        #mdified_Link_ID a combination of SLA_ID And Link_ID
        self.df['modified_Link_ID'] = self.df['SLA_ID'] + '_' + self.df['Link_ID'].astype(str)

        #Adding a UI to be used as pk         
        self.df['Unique_Link_Identifier_Invoice'] = self.df['Invoice_Reference'].astype(str) + '_' + self.df['Link_ID'].astype(str) + '_' + pd.to_datetime(self.df['SLA_Date']).dt.year.fillna(0).astype(int).astype(str).replace('0','0000')


        #Dropping redundant cols
        redundant_cols = ['SLA_ID', 'rank']
        self.df.drop(columns = redundant_cols, inplace = True)

        return self.df

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
        for pat in ['site', 'service']:
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
            

        # Removing any trailling or leading space and imputing nulls with 'Unknown' in Lastmile Col
        if 'Last Mile' in self.df.columns:
            self.df['Last Mile'] = self.df['Last Mile'].str.strip().fillna('Unknown')

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
        #self.df['SLA_ID'] = self.df.apply(lambda row: f"{row['SLA_Date'].year}-{row['SLA_Date'].month}-{self.sp}{row['rank']}", axis=1)
        self.df['SLA_ID'] = self.df.apply(lambda row: f"{row['SLA_Date'].year if pd.notnull(row['SLA_Date']) else '0000'}-{row['SLA_Date'].month if pd.notnull(row['SLA_Date']) else '00'}-{self.sp}{row['rank']}", axis=1)
        #Unique_Link_ID a combination of SLA_ID And Link_ID
        self.df['Unique_Link_Identifier_SLA'] = self.df['SLA_ID'] + '_' + self.df['Link_ID'].astype(str)


        # dropping redundant cols
        red_cols = ['rank']
        self.df.drop(columns = red_cols, inplace = True)

        return self.df
### Location_Cordinates
class location_cor:
    def __init__(self, df):
        self.df = df

    def location_cleaner(self):
        # Defining the partial Replacement
        partial_replacements = {
        'kra': '',
        '-': '',
        'internet': '',
        'ceragon': '',
        'wimax': '',
        'fiber': '',
        'loop': '',
        'microwave':'',
        'ppo': '',
        'pier': '',
        'fibre': ''
        }

        #Defining replacements for entire cellls
        full_replacement = {
            'sameer': 'Nairobi-Sameer Park',
            'hq' : 'Nairobi-Times Tower',
            'fixed' : 'Nairobi-Times Tower',
            'kixp' : 'Nairobi-Times Tower',
            'backhaul': 'Nairobi-Times Tower',
            'times tower' : 'Nairobi-Times Tower',
            'msa': 'Mombasa - Customs House',
            'city hall': 'Nairobi - City Hall',
            'kenya school': 'KESRA Nairobi',
            'kesra':'KESRA Nairobi',
            'fortiswestlands': 'Fortis westlands',
            'kopanga': 'kilindini'          

        } 
        



