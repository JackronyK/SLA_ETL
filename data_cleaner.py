import pandas as pd
import numpy as np

'''
Cleaner Module. 
It has 2 class
1. Invoice Cleaner used to Process the Invoice data frames 
2. SLA Cleaner used to process the SLA data Frames
'''
class InvoiceCleaner:
    def __init__(self, df):
        self.df = df

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

    def clean_data(self):
        self.drop_null_rows()
        self.drop_total_row()

        columns_wanted = {
            "Date": "Invoice Date",
            "ID": "Link ID",
            "Period": "Invoice Period",
            "Description": "Invoice Description",
            "Reference/No": self.pick_invoice_column_name(),
            "QRC": self.QRC_Finder()
        }

        self.df = self.df.loc[:, [value for value in columns_wanted.values()]]

        new_invoice_col_names = {
            "Invoice Date": "Invoice_Data",
            "Link ID": "Link_ID",
            "Invoice Period": "Invoice_Period",
            "Invoice Description": "Invoice_Description",
            self.pick_invoice_column_name(): "Invoice_Reference",
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
        if 'Invoice_Data' in self.df.columns:
            self.df['Invoice_Data'] = pd.to_datetime(self.df['Invoice_Data'])

        return self.df


class SLACleaner:
    def __init__(self, df):
        self.df = df

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
            self.excise_duty = 0.15
            self.VAT = 0.16
            self.df['QRC'] = np.where(
                self.df['Last Mile'].str.lower() == 'internet', # Internet taxation which includes both excise and VAT
                ((self.df[self.MRC_picker()] * (1+self.excise_duty)) * (1+self.VAT)*3),                
                # Multiprotocol label Switching (MPLS) which includes only VAT
                np.multiply(self.df[self.MRC_picker()] * (1+self.VAT),3)                

            )
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
            self.capacity_picker(): 'Capacity(Mbps)',
            self.MRC_picker(): 'MRC_Excl',
            self.comments_picker(): 'SLM_Comments'
        }
        self.df = self.df.rename(columns=SLA_New_Names)

        return self.df