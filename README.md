# ph-ee-connector-ams-mifos
Payment Hub Enterprise Edition connector for local AMS.  
By default local quote is disabled and only empty Zeebe workers are started.  
Currently supported AMS backends: (configure corresponding yml files to modify properties)
* Fineract 1.2 -> use spring profile fin12

2023-01-31

**Important:** Use filesystem path to set keystore for TLS Client configuration.
