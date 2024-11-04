# ReviseCareAI
Project for Microsoft Hackaton on Healthcare, AI, DataEngineering


# How to Get Started
1) Install Azure Fabric Environment
2) Create a workspace
3) Create a lakehouse with the name "LAKEHOUSE"
4) Import all the notebooks from this REPO
5) Attach lakehouse to each imported notebook as a default. It is important, otherwise, the process will raise the Error because it doesn't understand the target or source location.
6) Inside workspace, create a custom environment, with two preinstalled public libraries:
- openai
- googlemaps
- ipywidgets
7) Be sure, all the notebooks have the previously created environment as a default. It is important, otherwise you will see the error message about missed module(s).
8) Update the notebook "0_ReviseCare_Secrets" with the right keys for Azure Open AI Key, and Google Map API Key. According to the best practices, one of the ways is to create a key vault and import them from the Key vault
9) Run the 2_* notebook to process all the data and gather new ones for the configured hospitals

10) If you need to reconfigure which Hospitals should be included, run the notebook "1_ReviseCareAI_HospitalsInitialization" and copy its output into the first cell of the 2_ReviseCareAI_Reviews_Enriched

11) To view the reports, use 3_* and 4_* notebooks respectively. Run them, scroll down, and play with filters to see information for different hospitals

12) For expanding the functionality of the notebooks, learn the structure of 0_ReviseCareAI_Utils notebook. It has the main functions to work with. Also, observe the architecture pictures below, to understand holistically, how the solution works

# Data Flow Architecture Diagram of the Solution
![Architecture Diagram](resources/Architecture_ReviseCareAI-DataFlowDiagram.png)

# Component Diagram of the Solution
![Architecture Diagram](resources/Architecture_ReviseCareAI-ComponentDiagram.png)








