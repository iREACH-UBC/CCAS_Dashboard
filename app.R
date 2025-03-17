library(shiny)
library(leaflet)
library(dplyr)
library(readr)
library(lubridate)

# Define sensor locations by sensor id (update as needed)
sensor_locations <- list(
  "2021" = list(lat = 49.141444, lng = -123.10827),
  "2022" = list(lat = 49.141445, lng = -123.10822),
  "2023" = list(lat = 49.14143, lng = -123.10821),
  "2024" = list(lat = 49.141448, lng = -123.10826),
  "2026" = list(lat = 49.1414410, lng = -123.10823),
  "2030" = list(lat = 49.141449, lng = -123.10829),
  "2031" = list(lat = 49.141443, lng = -123.108211),
  "2032" = list(lat = 49.141442, lng = -123.10822),
  "2033" = list(lat = 49.141441, lng = -123.10828),
  "2034" = list(lat = 49.141446, lng = -123.10824),
  "2039" = list(lat = 49.141444, lng = -123.10822),
  "2040" = list(lat = 49.141443, lng = -123.10828),
  "2041" = list(lat = 49.141448, lng = -123.10827),
  "2042" = list(lat = 49.141446, lng = -123.10829),
  "2043" = list(lat = 49.141425, lng = -123.10825)
  # Add additional sensors here
)

# Function to choose marker color based on AQI
getAQIColor <- function(aqi) {
  aqi <- as.numeric(aqi)
  if (is.na(aqi)) return("gray")
  if (aqi <= 50) {
    return("green")
  } else if (aqi <= 100) {
    return("yellow")
  } else if (aqi <= 150) {
    return("orange")
  } else if (aqi <= 200) {
    return("red")
  } else if (aqi <= 300) {
    return("purple")
  } else {
    return("maroon")
  }
}

# Function to return a qualitative description for AQI as a length-one character vector.
getAQIDescription <- function(aqi) {
  aqi <- as.numeric(aqi)
  if (is.na(aqi)) return("No data available")
  if (aqi <= 50) {
    return("Good air quality")
  } else if (aqi <= 100) {
    return("Moderate air quality")
  } else if (aqi <= 150) {
    return("Unhealthy for sensitive groups")
  } else if (aqi <= 200) {
    return("Unhealthy air quality")
  } else if (aqi <= 300) {
    return("Very unhealthy air quality")
  } else {
    return("Hazardous air quality")
  }
}

loadCalibratedData <- function(sensor_ids) {
  data_list <- lapply(sensor_ids, function(sensor_id) {
    pattern <- paste0("^", sensor_id, "_calibrated_.*\\.csv$")
    files <- list.files("calibrated_data", pattern = pattern, full.names = TRUE)
    if (length(files) == 0) return(NULL)
    dates <- sapply(files, function(f) {
      parts <- unlist(strsplit(basename(f), "_"))
      as.Date(parts[3], format = "%Y-%m-%d")
    })
    latest_file <- files[which.max(dates)]
    
    # Read the CSV file
    df <- read_csv(latest_file, show_col_types = FALSE)
    
    # --- Debugging Code ---
    cat("DEBUG: Processing sensor", sensor_id, "from file:", latest_file, "\n")
    print(sapply(df, class))
    # ----------------------
    
    # Define the expected numeric columns (by name)
    numeric_cols <- c("AQI", "CO", "NO", "NO2", "O3", "CO2", "PM1.0", "PM2.5", "PM10",
                      "TE", "T", "RH", "WD", "WS", "PWR", "BATT", "CHRG", "RUN",
                      "SD", "RAW")
    
    # Force conversion for expected numeric columns
    for (col in numeric_cols) {
      if (col %in% names(df)) {
        original <- df[[col]]
        df[[col]] <- as.numeric(df[[col]])
        if (any(is.na(df[[col]]) & !is.na(original))) {
          warning(paste("Conversion to numeric resulted in NA for column", col, "in sensor", sensor_id))
        }
      }
    }
    
    # Additionally, force conversion for any columns whose names look like numeric values
    for (col in names(df)) {
      if (grepl("^-?[0-9.]+$", col)) {
        original <- df[[col]]
        df[[col]] <- as.numeric(df[[col]])
        if (any(is.na(df[[col]]) & !is.na(original))) {
          warning(paste("Conversion to numeric resulted in NA for column", col, "in sensor", sensor_id))
        }
      }
    }
    
    if (nrow(df) == 0) return(NULL)
    latest <- df %>% arrange(desc(DATE)) %>% slice(1)
    latest$sensor_id <- sensor_id
    latest
  })
  bind_rows(data_list)
}

# Load historical data (last 24 hours) for a sensor.
loadHistoricalData <- function(sensor_id) {
  pattern <- paste0("^", sensor_id, "_calibrated_.*\\.csv$")
  files <- list.files("calibrated_data", pattern = pattern, full.names = TRUE)
  if (length(files) == 0) return(NULL)
  dates <- sapply(files, function(f) {
    parts <- unlist(strsplit(basename(f), "_"))
    as.Date(parts[3], format = "%Y-%m-%d")
  })
  latest_file <- files[which.max(dates)]
  df <- read_csv(latest_file, show_col_types = FALSE)
  df <- df %>% mutate(DATE = as.POSIXct(DATE))
  df %>% filter(DATE >= Sys.time() - 24*3600)
}

# Define pollutants (CSV column names) and their units.
pollutants <- list(
  "CO" = "ppm",
  "NO" = "ppb",
  "NO2" = "ppb",
  "O3" = "ppb",
  "CO2" = "ppm",
  "PM1.0" = "µg/m³",
  "PM2.5" = "µg/m³",
  "PM10" = "µg/m³"
)

ui <- fluidPage(
  # Include external CSS from www/styles.css
  includeCSS("www/styles.css"),
  tags$head(
    tags$meta(charset = "utf-8")
  ),
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
    tabPanel("Home",
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
                            actionButton("list_page", "Detailed View", class = "nav-button", style = "background-image: url('list_image.png');"),
                            actionButton("info_page", "Info", class = "nav-button", style = "background-image: url('placeholder-image.jpg');")
                        )
                 )
               )
             )
    ),
    tabPanel("Map",
             fluidPage(
               fluidRow(
                 column(9, leafletOutput("airQualityMap", height = "85vh")),
                 column(3,
                        selectInput("sensor_select", "Select Sensor", choices = names(sensor_locations), selected = ""),
                        uiOutput("sensor_details"),
                        textOutput("last_update")
                 )
               )
             )
    ),
    tabPanel("Detailed View",
             fluidPage(
               # Dropdown for sensor selection on the List page.
               selectInput("list_sensor_select", "Select Sensor", choices = names(sensor_locations), selected = ""),
               uiOutput("sensor_info")
             )
    ),
    tabPanel("Info",
             fluidPage(
               h2("Information Page - Coming Soon")
             )
    )
  )
)

server <- function(input, output, session) {
  
  # Navigation between tabs.
  observeEvent(input$map_page, { updateNavbarPage(session, "navbar", selected = "Map") })
  observeEvent(input$list_page, { updateNavbarPage(session, "navbar", selected = "Detailed") })
  observeEvent(input$info_page, { updateNavbarPage(session, "navbar", selected = "Info") })
  
  sensor_data <- reactive({ loadCalibratedData(names(sensor_locations)) })
  
  output$airQualityMap <- renderLeaflet({
    m <- leaflet() %>% addTiles()
    df <- sensor_data()
    if(nrow(df) > 0) {
      df <- df %>%
        mutate(
          marker_color = sapply(AQI, getAQIColor),
          popup_text = paste0("<b>Sensor: ", sensor_id, "</b><br>",
                              "AQI: ", round(as.numeric(AQI), 1), "<br>")
        )
      for(i in 1:nrow(df)) {
        sensor_id <- df$sensor_id[i]
        loc <- sensor_locations[[sensor_id]]
        if(!is.null(loc)) {
          m <- m %>% addCircleMarkers(
            lng = loc$lng, lat = loc$lat,
            color = df$marker_color[i], radius = 8, fillOpacity = 0.8,
            popup = df$popup_text[i], layerId = sensor_id
          )
        }
      }
    }
    m
  })
  
  observeEvent(input$sensor_select, {
    sensor_id <- input$sensor_select
    if (sensor_id != "" && sensor_id %in% names(sensor_locations)) {
      loc <- sensor_locations[[sensor_id]]
      leafletProxy("airQualityMap") %>% setView(lng = loc$lng, lat = loc$lat, zoom = 12)
    }
  })
  
  output$last_update <- renderText({
    df <- sensor_data()
    if(nrow(df) == 0) {
      "No data available"
    } else {
      last_update <- max(as.POSIXct(df$DATE), na.rm = TRUE)
      last_update <- force_tz(last_update, tzone="UTC")
      last_update_pst <- last_update - lubridate::hours(16)
      paste("Last updated:", format(last_update_pst, "%Y-%m-%d %H:%M"), "PST")
    }
  })
  
  output$sensor_details <- renderUI({
    selected_sensor <- if(!is.null(input$sensor_select) && input$sensor_select != "") {
      input$sensor_select
    } else if(!is.null(input$airQualityMap_marker_click)) {
      input$airQualityMap_marker_click$id
    } else { 
      NULL 
    }
    if(is.null(selected_sensor)) return(NULL)
    df <- sensor_data()
    sensor_row <- df %>% filter(sensor_id == selected_sensor)
    if(nrow(sensor_row) == 0) return(NULL)
    wellPanel(
      h4(paste("Sensor", selected_sensor, "Details")),
      tags$table(class = "data-table",
                 tags$thead(
                   tags$tr(
                     tags$th("Pollutant"),
                     tags$th("Value"),
                     tags$th("Unit")
                   )
                 ),
                 tags$tbody(
                   tags$tr(
                     tags$td("CO"),
                     tags$td(round(as.numeric(sensor_row$CO)[1], 1)),
                     tags$td("ppm")
                   ),
                   tags$tr(
                     tags$td("NO"),
                     tags$td(round(as.numeric(sensor_row$NO)[1], 1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("NO2"),
                     tags$td(round(as.numeric(sensor_row$NO2)[1], 1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("O3"),
                     tags$td(round(as.numeric(sensor_row$O3)[1], 1)),
                     tags$td("ppb")
                   ),
                   tags$tr(
                     tags$td("CO2"),
                     tags$td(round(as.numeric(sensor_row$CO2)[1], 1)),
                     tags$td("ppm")
                   ),
                   tags$tr(
                     tags$td("PM1.0"),
                     tags$td(round(as.numeric(sensor_row$`PM1.0`)[1], 1)),
                     tags$td("µg/m³")
                   ),
                   tags$tr(
                     tags$td("PM2.5"),
                     tags$td(round(as.numeric(sensor_row$`PM2.5`)[1], 1)),
                     tags$td("µg/m³")
                   ),
                   tags$tr(
                     tags$td("PM10"),
                     tags$td(round(as.numeric(sensor_row$PM10)[1], 1)),
                     tags$td("µg/m³")
                   )
                 )
      )
    )
  })
  
  # New List page UI: sensor selection, qualitative description, expandable pollutant details, and 24-hour graph.
  output$sensor_info <- renderUI({
    req(input$list_sensor_select)
    sensor_id <- input$list_sensor_select
    df <- sensor_data()
    sensor_row <- df %>% filter(sensor_id == sensor_id)
    if(nrow(sensor_row) == 0) return("No sensor data available.")
    
    # Get a single qualitative description for the AQI.
    description <- getAQIDescription(sensor_row$AQI[1])
    
    # Build pollutant concentration items ensuring each value is length-one.
    pollutant_ui <- lapply(names(pollutants), function(poll) {
      value <- round(as.numeric(sensor_row[[poll]][1]), 1)
      div(class = "pollutant-item", paste0(poll, ": ", value, " ", pollutants[[poll]]))
    })
    
    tagList(
      div(class = "sensor-info-box",
          h4(paste("Sensor", sensor_id, "Air Quality")),
          p(description),
          tags$details(
            tags$summary("More info"),
            div(
              tagList(pollutant_ui),
              tags$details(
                tags$summary("Show 24-hour AQI graph"),
                plotOutput("selected_sensor_plot")
              )
            )
          )
      )
    )
  })
  
  # Render the 24-hour AQI graph for the selected sensor.
  output$selected_sensor_plot <- renderPlot({
    req(input$list_sensor_select)
    sensor_id <- input$list_sensor_select
    hist_data <- loadHistoricalData(sensor_id)
    req(nrow(hist_data) > 0)
    hist_data <- hist_data %>% mutate(DATE = as.POSIXct(DATE))
    aqi_values <- as.numeric(hist_data$AQI)
    plot(hist_data$DATE, aqi_values, type = "l", lwd = 2,
         xlab = "Time", ylab = "AQI",
         main = paste("24-hour AQI for Sensor", sensor_id))
  })
}

shinyApp(ui, server)
