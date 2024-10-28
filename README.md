# ReviseCareAI
Project for Microsoft Hackaton on Healthcare, AI, DataEngineering


# How to Get Started
1) Install Azure Fabric Environment
2) Create a workspace
3) Inside workspace, create a custom environment, with two preinstalled public libraries:
- openai
- googlemaps
4) Create a lakehouse with the name "LAKEHOUSE"
5) Import all the notebooks from this REPO
6) Attach lakehouse to each imported notebook as a default
7) Update the notebook "0_ReviseCare_Secrets" with the right keys for Azure Open AI Key, and Google Map API Key. According to the best practices, one of the ways is to create a key vault and import them from the Key vault
8) Run the MASTER_NOTEBOOK to process all the data and gather new ones for the configured hospitals
9) If you need to reconfigure which Hospitals should be included, run the notebook "1_ReviseCareAI_HospitalsInitialization" and copy its input into the first cell of the 2_ReviseCareAI_Reviews_Enriched
