# ph-ee-connector-ams-mifos
Payment Hub Enterprise Edition connector for AMS.

## Spring profiles:

* **dev:**: All current developments

## External variables:

* **SPRING_PROFILES_ACTIVE**: the actual Spring profile the AMS Connector is being launched with
* **FINERACT_URL**: The URL for the Apache Fineract provider the current AMS Connector instance is connecting to
* **FINERACT_AUTH_USER**: The username component to the HTTP Authorization header towards Apache Fineract
* **FINERACT_AUTH_PASSWORD**: The password component to the HTTP Authorization header towards Apache Fineract
* **ZEEBE_CLIENT_BROKER_GATEWAY_ADDRESS**: The URL of the Camunda Zeebe gateway to register and manage workers
* **AMS_LOCAL_KEYSTORE_PATH**: The path to the keystore file for HTTPS connections
* **AMS_LOCAL_KEYSTORE_PASSWORD**: The password to the keystore file for HTTPS connections
* **TENANT_CONFIGS**: See next section


## Tenant configs

Each step within each payment process flow is referenced by a unique payment type ID that is defined by an Apache Fineract installation instance. The tenant config is a lookup mechanism, that maps each payment scheme + payment operation to its corresponding payment type ID.


Currently supported AMS backends: (configure corresponding yml files to modify properties)
* Fineract 1.2 -> use spring profile fin12

2023-02-17 14:45

**Important:** Use filesystem path to set keystore for TLS Client configuration.