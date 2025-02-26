library(shiny)
library(leaflet)
library(dplyr)
library(readr)
library(lubridate)

# Define sensor locations by sensor id (update as needed)
sensor_locations <- list(
  "2035" = list(lat = 49.2827, lng = -123.1207)
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
    #airQualityMap { width: 100%; height: 75vh; }
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
          column(9, leafletOutput("airQualityMap")),
          column(3,
                 h4("Last Data Update (PST)"),
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
  
  # Refresh the data once every hour using invalidateLater (3600000 ms)
  sensor_data <- reactive({
    invalidateLater(3600000, session)
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
            "AQI: ", round(AQI, 1), "<br>",
            "CO: ", CO, "<br>",
            "NO: ", NO, "<br>",
            "NO₂: ", NO2, "<br>",
            "O₃: ", O3, "<br>",
            "CO₂: ", CO2, "<br>",
            "PM1.0: ", `PM1.0`, "<br>",
            "PM2.5: ", `PM2.5`, "<br>",
            "PM10: ", PM10, "<br>",
            "Temp: ", T, " °C<br>",
            "RH: ", RH, " %"
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
            popup = df$popup_text[i]
          )
        }
      }
    }
    m
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
}

shinyApp(ui, server)

