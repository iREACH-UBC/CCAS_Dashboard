library(shiny)
library(leaflet)
library(dplyr)
library(readr)
library(lubridate)

# Define sensor locations by sensor id (update as needed)
sensor_locations <- list(
  "2035" = list(lat = 49.2827, lng = -123.1207),
  "2029" = list(lat = 49.4, lng = -123.7)
  # Add additional sensors here
)

# Function to choose marker color based on AQI
getAQIColor <- function(aqi) {
  if (is.na(aqi)) return("gray")
  if (aqi <= 50) {
    "green"
  } else if (aqi <= 100) {
    "yellow"
  } else if (aqi <= 150) {
    "orange"
  } else if (aqi <= 200) {
    "red"
  } else if (aqi <= 300) {
    "purple"
  } else {
    "maroon"
  }
}

# Function to load calibrated data for a given list of sensor IDs.
# This function looks for files in the "calibrated_data" folder.
loadCalibratedData <- function(sensor_ids) {
  data_list <- lapply(sensor_ids, function(sensor_id) {
    # Assume filenames are of the format: sensor_calibrated_YYYY-MM-DD_to_YYYY-MM-DD.csv
    pattern <- paste0("^", sensor_id, "_calibrated_.*\\.csv$")
    files <- list.files("calibrated_data", pattern = pattern, full.names = TRUE)
    if (length(files) == 0) return(NULL)
    # Pick the file with the most recent start date
    dates <- sapply(files, function(f) {
      fname <- basename(f)
      parts <- unlist(strsplit(fname, "_"))
      as.Date(parts[3], format = "%Y-%m-%d")
    })
    latest_file <- files[which.max(dates)]
    df <- read_csv(latest_file, show_col_types = FALSE)
    if (nrow(df) == 0) return(NULL)
    latest <- df %>% arrange(desc(DATE)) %>% slice(1)
    latest$sensor_id <- sensor_id
    return(latest)
  })
  bind_rows(data_list)
}

ui <- fluidPage(
  tags$head(tags$style(HTML("
    body { margin: 0; padding: 0; overflow-x: hidden; }
    .title-bar { width: 100vw; background-color: #002145; color: white; padding: 20px; display: flex; justify-content: space-between; align-items: center; box-sizing: border-box; margin: 0; }
    .title-left { font-size: 20px; font-weight: bold; margin-left: 0; padding-left: 10px; }
    .title-center { font-size: 24px; font-weight: bold; text-align: center; flex-grow: 1; }
    .title-right { display: flex; flex-direction: column; align-items: center; margin-right: 10px; }
    .nav-button { width: 30vw; height: 30vw; max-width: 300px; max-height: 300px; background-size: cover; background-position: center; border: none; cursor: pointer; font-size: 24px; text-align: center; color: white; font-weight: bold; opacity: 0.8; display: flex; align-items: center; justify-content: center; text-shadow: 2px 2px 4px black; }
    .nav-button:hover { opacity: 1; text-shadow: none; }
    .button-container { display: flex; justify-content: space-around; margin-top: 20px; }
  "))),
  div(class = "title-bar", 
      div(class = "title-left", "iREACH Laboratory"),
      div(class = "title-center", "Community Cleaner Air Spaces"),
      div(class = "title-right",
          img(src = "image1.png", height = "50px"),
          img(src = "image2.png", height = "50px")
      )
  ),
  navbarPage(
    id = "navbar",
    "Air Quality Dashboard",
    tabPanel(
      "Home",
      fluidPage(
        fluidRow(
          column(12, 
                 p("This dashboard displays nothing of interest at this point in time."),
                 div(id = "advisories", style = "background-color: #f8d7da; padding: 10px; border-radius: 5px;",
                     strong("Active Air Quality Advisories:"),
                     p("No advisories are currently active.")
                 ),
                 br(),
                 div(class = "button-container",
                     actionButton("map_page", "View Map", class = "nav-button", style = "background-image: url('map.png');"),
                     actionButton("list_page", "View List", class = "nav-button", style = "background-image: url('placeholder-image.jpg');"),
                     actionButton("info_page", "Info", class = "nav-button", style = "background-image: url('placeholder-image.jpg');")
                 )
          )
        )
      )
    ),
    tabPanel(
      "Map",
      fluidPage(
        fluidRow(
          column(9, leafletOutput("airQualityMap", height = "85vh")),  # Set explicit height for map
          column(3,
                 # At the top, a drop down to select sensor
                 selectInput("sensor_select", "Select Sensor", choices = names(sensor_locations), selected = ""),
                 # Sensor details display area
                 uiOutput("sensor_details"),
                 # At the bottom, last update message
                 textOutput("last_update")
          )
        )
      )
    ),
    tabPanel(
      "List",
      fluidPage(
        h2("List View - Coming Soon")
      )
    ),
    tabPanel(
      "Info",
      fluidPage(
        h2("Information Page - Coming Soon")
      )
    )
  )
)

server <- function(input, output, session) {
  
  observeEvent(input$map_page, {
    updateNavbarPage(session, "navbar", selected = "Map")
  })
  observeEvent(input$list_page, {
    updateNavbarPage(session, "navbar", selected = "List")
  })
  observeEvent(input$info_page, {
    updateNavbarPage(session, "navbar", selected = "Info")
  })
  
  # Load the most recent data for each sensor without an automatic refresh.
  sensor_data <- reactive({
    loadCalibratedData(names(sensor_locations))
  })
  
  output$airQualityMap <- renderLeaflet({
    m <- leaflet() %>% addTiles()
    df <- sensor_data()
    if(nrow(df) > 0) {
      df <- df %>%
        mutate(
          marker_color = sapply(AQI, getAQIColor),
          popup_text = paste0(
            "<b>Sensor: ", sensor_id, "</b><br>",
            "AQI: ", round(AQI, 1), "<br>"
          )
        )
      
      for(i in 1:nrow(df)) {
        sensor_id <- df$sensor_id[i]
        loc <- sensor_locations[[sensor_id]]
        if(!is.null(loc)) {
          m <- m %>% addCircleMarkers(
            lng = loc$lng,
            lat = loc$lat,
            color = df$marker_color[i],
            radius = 8,
            fillOpacity = 0.8,
            popup = df$popup_text[i],
            layerId = sensor_id  # Added layerId so we can identify the sensor on click
          )
        }
      }
    }
    m
  })
  
  # Observer to center map when a sensor is selected from the drop down
  observeEvent(input$sensor_select, {
    sensor_id <- input$sensor_select
    if (sensor_id != "" && sensor_id %in% names(sensor_locations)) {
      loc <- sensor_locations[[sensor_id]]
      leafletProxy("airQualityMap") %>% setView(lng = loc$lng, lat = loc$lat, zoom = 12)
    }
  })
  
  # Render the last update time in PST using lubridate
  output$last_update <- renderText({
    df <- sensor_data()
    if (nrow(df) == 0) {
      "No data available"
    } else {
      last_update <- max(as.POSIXct(df$DATE), na.rm = TRUE)
      last_update <- force_tz(last_update, tzone = "UTC")
      last_update_pst <- last_update - lubridate::hours(16) # Adjust as needed
      paste("Last updated:", format(last_update_pst, "%Y-%m-%d %H:%M"), "PST")
    }
  })
  
  # Render pollutant concentrations based on sensor selection or marker click
  output$sensor_details <- renderUI({
    # Prioritize sensor selected from dropdown if available,
    # otherwise fall back to the sensor marker click.
    selected_sensor <- if (!is.null(input$sensor_select) && input$sensor_select != "") {
      input$sensor_select
    } else if (!is.null(input$airQualityMap_marker_click)) {
      input$airQualityMap_marker_click$id
    } else {
      NULL
    }
    if (is.null(selected_sensor)) return(NULL)
    
    # Get sensor data for the selected sensor
    df <- sensor_data()
    sensor_row <- df %>% filter(sensor_id == selected_sensor)
    if(nrow(sensor_row) == 0) return(NULL)
    
    # Beautified display with units using a table inside a wellPanel;
    # each numeric value is rounded to 1 decimal place and table cells are padded.
    wellPanel(
      h4(paste("Sensor", selected_sensor, "Details")),
      tags$table(style="width:100%;",
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("CO:")),
                   tags$td(style="padding: 5px;", round(sensor_row$CO, 1)),
                   tags$td(style="padding: 5px;", "ppm")
                 ),
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("NO:")),
                   tags$td(style="padding: 5px;", round(sensor_row$NO, 1)),
                   tags$td(style="padding: 5px;", "ppb")
                 ),
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("NO₂:")),
                   tags$td(style="padding: 5px;", round(sensor_row$NO2, 1)),
                   tags$td(style="padding: 5px;", "ppb")
                 ),
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("O₃:")),
                   tags$td(style="padding: 5px;", round(sensor_row$O3, 1)),
                   tags$td(style="padding: 5px;", "ppb")
                 ),
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("CO₂:")),
                   tags$td(style="padding: 5px;", round(sensor_row$CO2, 1)),
                   tags$td(style="padding: 5px;", "ppm")
                 ),
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("PM1.0:")),
                   tags$td(style="padding: 5px;", round(sensor_row$`PM1.0`, 1)),
                   tags$td(style="padding: 5px;", "µg/m³")
                 ),
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("PM2.5:")),
                   tags$td(style="padding: 5px;", round(sensor_row$`PM2.5`, 1)),
                   tags$td(style="padding: 5px;", "µg/m³")
                 ),
                 tags$tr(
                   tags$td(style="padding: 5px;", strong("PM10:")),
                   tags$td(style="padding: 5px;", round(sensor_row$PM10, 1)),
                   tags$td(style="padding: 5px;", "µg/m³")
                 )
      )
    )
  })
}

shinyApp(ui, server)
