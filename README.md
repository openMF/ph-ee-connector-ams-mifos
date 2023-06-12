# ph-ee-connector-ams-mifos
Payment Hub Enterprise Edition connector for AMS.

## Spring profiles:

* **dev:**: All current developments

## External variables:

* **SPRING_PROFILES_ACTIVE**: the actual Spring profile the AMS Connector is being launched with.
    * Default value: `default`
    * Example: `dev`
* **FINERACT_URL**: The URL for the Apache Fineract provider the current AMS Connector instance is connecting to
    * Default value: empty string
    * Example: `http://fineract.fineract:8080/fineract-provider/api/v1`
* **FINERACT_AUTH_USER**: The username component to the HTTP Authorization header towards Apache Fineract
    * Default value: empty string
    * Example: `fineract`
* **FINERACT_AUTH_PASSWORD**: The password component to the HTTP Authorization header towards Apache Fineract
    * Default value: empty string
    * Example: `passwd`
* **ZEEBE_CLIENT_BROKER_GATEWAY_ADDRESS**: The URL of the Camunda Zeebe gateway to register and manage workers
    * Default value: empty string
    * Example: `zeebe-gateway:26500`
* **AMS_LOCAL_KEYSTORE_PATH**: The path to the keystore file for HTTPS connections
    * Default value: empty string
    * Example: `keystore.jks`
* **AMS_LOCAL_KEYSTORE_PASSWORD**: The password to the keystore file for HTTPS connections
    * Default value: empty string
    * Example: `changeit`
* **TENANT_CONFIGS**: See next section


## Tenant configs

Each step within each payment process flow is referenced by a unique payment type ID that is defined by an Apache Fineract installation instance. The tenant config is a lookup mechanism, that maps each payment scheme + payment operation to its corresponding payment type ID.

The tenant configs contains a tenant specific list of operations in the following structure:

```
{
    "tenant-name": [
        {"Operation": "SCHEME.operation1.name", "PaymentTypeCode": "PTC01", "FineractId": 1},
        {"Operation": "SCHEME.operation2.name", "PaymentTypeCode": "PTC02", "FineractId": 2}
    ]
}
```

In this example we have a tenant referred to as `tenant-name`, and the `SCHEME.operation1.name` operation has the Fineract ID `1` mapped to it. Each Fineract ID is looked up in the code based on the concatenation of the payment scheme and the operation name, and each such entry contains a human readable `PaymentTypeCode` that allows the configuration to be checked more easily. The `FineractId` values are then utilized in the HTTP calls towards Apache Fineract as per the specification of event accounting.

When initializing Apache Fineract, the payment types are registered, each generating its own corresponding Payment Type ID. The proper init script in Postman can store these Payment Type IDs, with which in turn the tenant configs can be populated.


Currently supported AMS backends: (configure corresponding yml files to modify properties)
* Fineract 1.2 -> use spring profile fin12

2023-02-17 14:45

**Important:** Use filesystem path to set keystore for TLS Client configuration.