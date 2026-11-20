# Azure Data Factory

This repository contains the code that runs on Azure Data Factory (ADF). ADF is used only for the ingestion of data.

This repository also contains the ingestion catalog sync pipeline for Databricks. This pipeline is used to sync the datasets sourced from ADF to the Databricks `ingestion_*` catalog.

## Developing in this repo for ADF

This repo is linked to an ADF instance in each of our environments:

* dev: [adf-mx-data-adf-inges-dev-01](https://adf.azure.com/en/home?factory=%2Fsubscriptions%2F312989a6-ad5b-42b5-b553-4bb71ea31df6%2FresourceGroups%2Frg-mx-data-adf-inges-dev%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2Fadf-mx-data-adf-inges-dev-01)
* uat: [adf-mx-data-adf-inges-uat-01]() #TODO
* preprod: [adf-mx-data-adf-inges-preprd-01]() #TODO
* production: [adf-mx-data-adf-inges-prd-01]() #TODO

1. Development is performed in the `dev` instance. Start by creating a feature branch based on the `main` branch.
2. Make your desired changes.
3. Create a pull request (PR) against the `main` branch.
4. Once the PR is created, the code is automatically deployed to the `uat` environment for initial testing.
5. Once the CI Pipeline succeeds, ask for a code review so the appropriate team members can review the PR.
6. Once the PR is approved and merged to the `main` branch, a CD Pipeline is triggered to deploy the changes to the `preprod` environment for user acceptance testing.
7. After validation in the `preprod` environment, the changes are ready to be merged into the `release` branch.
8. Once merged to the `release` branch, the changes will be deployed to the `production` environment.
