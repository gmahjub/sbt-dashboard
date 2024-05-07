# Project Description
- Name: project_markets_dash

# Objective
- Build a multi-page app 
- Extract, parse, and clean data 
- Compute stats needed
- Data Visualization using Plotly
- Front-End: Plotly Dash app - deployed on Google Cloud Platform

# Data Source
- Prod Dashboard file on Google Workspace, formatted as a Google Sheet.
- Comning soon: treasury.gov for treasury yields, par nominal yields

# Type of analysis
- Tracking various SPX, ES, MES, SPY models and analyzing performance versus factors, such as rate volatility.
- Rate volatility factors are synthetic model factors
- Soon to add MOVE and other market based volatility indicators.

# Live app
- https://stealbasis.co

# Building and Running on GCP
- Commands are:
  - gcloud builds submit --tag gcr.io/axial-crane-382617/sbt-dashboard --project=axial-crane-382617
  - gcloud run deploy --image gcr.io/axial-crane-382617/sbt-dashboard --platform managed 
  - --project=axial-crane-382617 --allow-unauthenticated

