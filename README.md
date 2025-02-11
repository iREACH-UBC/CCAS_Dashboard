# .gitignore
# Ignore R-related files
.Rhistory
.RData
.Rproj.user/

# Ignore data files
/data/*
!data/.gitkeep

# Ignore OS-generated files
.DS_Store
Thumbs.db

# README.md
# Air Quality Dashboard

This R Shiny app displays air pollution data collected from various monitors. Users can view data on a map, in list format, or read additional information.

## Features
- Landing page with active air quality advisories
- Interactive map displaying processed data
- List and Info pages (to be implemented)

## Getting Started
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/air-quality-dashboard.git
   ```
2. Navigate to the project directory:
   ```bash
   cd air-quality-dashboard
   ```
3. Run the app in R:
   ```R
   shiny::runApp()
   ```