
# SLA & Invoice Data Cleaning Pipeline  

## Overview  

This project is a robust **SLA & Invoice Data Cleaning Pipeline**, developed as a complete end-to-end solution to revolutionize SLA (Service Level Agreements) and invoice management at **KRA**. By streamlining operations and automating previously manual processes, this pipeline has significantly enhanced efficiency, improved data accuracy, and provided actionable insights through advanced visualization and reliable data storage.  

## Key Features  

1. **Modularized Architecture**  
   - Designed with modular Python components for scalability and maintainability.  
   - Each module can be independently checked and improved without affecting the entire pipeline.  

   **Modules Include:**  
   - `InvoiceCleaner` and `SLACleaner`: Specialized modules for cleaning and preprocessing SLA and invoice data.  
   - `pre_processor`: Handles pre-cleaning tasks like handling missing values and outliers.  
   - `LocationCor`: Enriches datasets with geographical coordinates using APIs.  
   - `DB_Manager` and `DataFrameToSQL`: Manages database interactions and facilitates seamless data integration into **MySQL**.  

2. **Environment Setup Automation**  
   - Includes an automated guide for setting up the Python environment with all necessary packages.  

3. **Dynamic Mapping & Visualization**  
   - Interactive visualizations built with **Power BI**, showcasing geospatial and operational insights.  
   - Geographical distribution of service shops, performance metrics, and invoice summaries presented dynamically.  

4. **Scalable Storage Solution**  
   - Data stored and managed in **MySQL**, ensuring scalability, security, and fast querying capabilities.  

5. **Complete End-to-End Pipeline**  
   - From raw data ingestion to cleaned outputs and insightful dashboards, this pipeline automates the entire workflow.  

---

## Impact on Operations  

The project has brought transformative changes to the SLM department at **KRA**:  
- **Time Efficiency**: Reduced manual effort for data cleaning and preparation by over 50%.  
- **Data Accuracy**: Enhanced data integrity and ensured compliance with audit standards.  
- **Visualization & Insights**: Leveraged **Power BI** dashboards for actionable, real-time insights into SLA performance and invoice trends.  
- **Streamlined Processes**: Improved invoice tracking and SLA performance analysis, enabling better decision-making and proactive management.  
- **Modular Scalability**: With its modularized design, the pipeline can be easily extended and updated as business requirements evolve.  

---

## Technologies Used  

- **Programming**: Python for data processing and automation, utilizing the following libraries:  
  - `pandas`, `numpy` for data manipulation.  
  - `dotenv` for environment variable management.  
  - `glob` for file handling.  
- **Visualization**: Power BI for creating dynamic and interactive dashboards.  
- **Database**: MySQL for efficient data storage and querying.  
- **APIs**: Geo-coordinates retrieved via APIs like OpenCage, MapQuest, and Google Maps.  
- **Environment Management**: Automated setup using pip and scripts for seamless deployment.  

---

## Getting Started  

### Prerequisites  

1. Ensure you have Python 3.7 or higher installed.  
2. Install the necessary Python packages using the provided script:  

```bash  
!pip install wget
file_url = 'https://raw.githubusercontent.com/JackronyK/SLA_ETL/main/Final%20Model/python_packages_requirements.txt'
!wget -O Python_Package_Requirements.txt $file_url

with open('Python_Package_Requirements.txt', 'r') as file:
    packages = file.readlines()

for package in packages:
    try:
        !pip install $package
    except Exception as e:
        print(f"Failed to install {package.strip()}: {str(e)}")
```

### Usage  

1. Clone the repository:  
   ```bash  
   git clone https://github.com/JackronyK/SLA_Invoice_Pipeline.git  
   cd SLA_Invoice_Pipeline  
   ```  

2. Set up the Python environment using the notebook instructions.  

3. Update the `.env` file with appropriate file paths and database credentials.  

4. Run the pipeline to process SLA and invoice data and generate dashboard-ready outputs.  

5. Load the processed data into **Power BI** to view interactive visualizations.  

---

## Visuals  

### Dashboard Preview  
![Power BI Dashboard](https://www.ascend.io/wp-content/uploads/2023/03/data-pipeline-automation.jpg)  

---

## Contributing  

Contributions are welcome! Fork this repository, submit pull requests, or open issues to share your ideas.  

---

## License  

This project is licensed under the [MIT License](LICENSE).  

---

## Acknowledgments  

Special thanks to the **SLM team at KRA** for their insights and collaboration that shaped this project into a powerful tool for operational efficiency.  

---  

