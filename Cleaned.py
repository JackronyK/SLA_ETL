import pandas as pd

class InvoiceCleaner:
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
    
    def clean_invoice_data(self):
        self.drop_null_rows()
        self.drop_total_row()
        
        columns_wanted = {
            "Date": "Invoice Date",
            "ID": "Link ID",
            "Period": "Invoice Period",
            "Descriprion": "Invoice Description",
            "QRC": self.QRC_Finder(),
            "Reference/ no": self.pick_invoice_no_column_name()
        }

        self.df = self.df.loc[:, [value for value in columns_wanted.values()]]