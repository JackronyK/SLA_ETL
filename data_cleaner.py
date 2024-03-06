import pandas as pd

class DataCleaner:
    def __init__(self, df):
        self.df = df

    def drop_null_rows(self):
        self.df.dropna(how='all', inplace=True)

    def drop_total_row(self):
        if "No." in self.df.columns:
            self.df.drop(self.df[self.df["No."] == "Total"].index, inplace=True)

    def pick_invoice_no_column_name(self):
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

    def col_of_interest(self):
        self.drop_null_rows()
        self.drop_total_row()

        columns_wanted = {
            "Date": "Invoice Date",
            "ID": "Link ID",
            "Period": "Invoice Period",
            "Description": "Invoice Description",            
            "Reference/No": self.pick_invoice_no_column_name(),
            "QRC": self.QRC_Finder()
        }

        self.df = self.df.loc[:, [value for value in columns_wanted.values()]]


    def col_renaming(self):
        self.col_of_interest()

        new_col_names = {
            "Invoice Date": "Invoice_Data",
            "Link ID": "Link_ID",
            "Invoice Period": "Invoice_Period",
            "Invoice Description":"Invoice_Description",           
            self.pick_invoice_no_column_name(): "Invoice_Reference",
             self.QRC_Finder(): "Total_QRC"
        }
        self.df = self.df.rename(columns = new_col_names)

    def clean_invoice_data(self):
        self.col_renaming()
        # Convert 'Invoice Date' to datetime
        if 'Invoice_Date' in self.df.columns:
            self.df['Invoice_Date'] = pd.to_datetime(self.df['Invoice_Date'])

        # Adjust 'Link ID' based on its current type
        if 'Link ID' in self.df.columns:
            if self.df['Link_ID'].dtype == 'float':
                self.df['Link_ID'] = self.df['Link_ID'].astype('int').astype('str')
            elif self.df['Link_ID'].dtype == 'int':
                self.df['Link_ID'] = self.df['Link_ID'].astype('str')
            # If 'Link ID' is already object, no conversion is needed
        return self.df